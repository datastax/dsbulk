/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.commons;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isValueFromReferenceConfig;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.CompressedIOUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** A parent class for connectors that read from and write to text-based files. */
public abstract class AbstractFileBasedConnector implements Connector {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileBasedConnector.class);

  protected static final String URL = "url";
  protected static final String URLFILE = "urlfile";
  protected static final String COMPRESSION = "compression";
  protected static final String ENCODING = "encoding";
  protected static final String FILE_NAME_PATTERN = "fileNamePattern";
  protected static final String SKIP_RECORDS = "skipRecords";
  protected static final String MAX_RECORDS = "maxRecords";
  protected static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  protected static final String RECURSIVE = "recursive";
  protected static final String FILE_NAME_FORMAT = "fileNameFormat";

  protected boolean read;
  protected List<URL> urls;
  protected List<Path> roots = new ArrayList<>();
  protected List<URL> files = new ArrayList<>();
  protected Charset encoding;
  protected String compression;
  protected String fileNameFormat;
  protected boolean recursive;
  protected String pattern;
  protected long skipRecords;
  protected long maxRecords;
  protected int resourceCount;
  protected Scheduler scheduler;
  protected int maxConcurrentFiles;
  protected List<RecordWriter> writers;
  protected AtomicInteger counter;

  // Public API

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public void configure(@NonNull Config settings, boolean read) {
    this.read = read;
    urls = loadURLs(settings);
    encoding = ConfigUtils.getCharset(settings, ENCODING);
    compression = settings.getString(COMPRESSION);
    if (!CompressedIOUtils.isSupportedCompression(compression, read)) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid value for connector.csv.%s, valid values: %s, got: '%s'",
              COMPRESSION,
              String.join(",", CompressedIOUtils.getSupportedCompressions(read)),
              compression));
    }
    pattern = settings.getString(FILE_NAME_PATTERN);
    if (!CompressedIOUtils.isNoneCompression(compression)
        && isValueFromReferenceConfig(settings, FILE_NAME_PATTERN)) {
      pattern = pattern + CompressedIOUtils.getCompressionSuffix(compression);
    }
    fileNameFormat = settings.getString(FILE_NAME_FORMAT);
    if (!CompressedIOUtils.isNoneCompression(compression)
        && isValueFromReferenceConfig(settings, FILE_NAME_FORMAT)) {
      fileNameFormat = fileNameFormat + CompressedIOUtils.getCompressionSuffix(compression);
    }
    recursive = settings.getBoolean(RECURSIVE);
    maxConcurrentFiles = ConfigUtils.getThreads(settings, MAX_CONCURRENT_FILES);
    skipRecords = settings.getLong(SKIP_RECORDS);
    maxRecords = settings.getLong(MAX_RECORDS);
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      processURLsForRead();
    } else {
      processURLsForWrite();
      counter = new AtomicInteger(0);
    }
  }

  @NonNull
  @Override
  public Publisher<Record> read() {
    assert read;
    return Flux.concat(
            Flux.fromIterable(roots).flatMap(this::scanRootDirectory), Flux.fromIterable(files))
        .flatMap(url -> readSingleFile(url).transform(this::applyPerFileLimits));
  }

  @NonNull
  @Override
  public Publisher<Publisher<Record>> readByResource() {
    assert read;
    return Flux.concat(
            Flux.fromIterable(roots).flatMap(this::scanRootDirectory), Flux.fromIterable(files))
        .map(this::readSingleFile);
  }

  @NonNull
  @Override
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    assert !read;
    writers = new CopyOnWriteArrayList<>();
    if (!roots.isEmpty() && maxConcurrentFiles > 1) {
      return upstream -> {
        ThreadFactory threadFactory = new DefaultThreadFactory(getConnectorName() + "-connector");
        scheduler = Schedulers.newParallel(maxConcurrentFiles, threadFactory);
        for (int i = 0; i < maxConcurrentFiles; i++) {
          writers.add(newSingleFileWriter());
        }
        return Flux.from(upstream)
            .parallel(maxConcurrentFiles)
            .runOn(scheduler)
            .groups()
            .flatMap(
                records -> {
                  Integer key = records.key();
                  assert key != null;
                  return records.transform(writeRecords(writers.get(key)));
                },
                maxConcurrentFiles);
      };
    } else {
      return upstream -> {
        RecordWriter writer = newSingleFileWriter();
        writers.add(writer);
        return Flux.from(upstream).transform(writeRecords(writer));
      };
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (writers != null) {
      IOException e = null;
      for (RecordWriter writer : writers) {
        try {
          writer.close();
        } catch (IOException e1) {
          if (e == null) {
            e = e1;
          } else {
            e.addSuppressed(e1);
          }
        }
      }
      if (e != null) {
        throw new UncheckedIOException(e);
      }
    }
  }

  // Protected members

  /**
   * Returns the connector name, e.g. "csv" for the CSV connector. This should match the
   * programmatic connector name and also its configuration namespace; that is, if this method
   * returns "foo", then the connector configuration namespace is expected to be {@code
   * connector.foo}.
   */
  @NonNull
  protected abstract String getConnectorName();

  /**
   * Reads a single text file accessible through the given URL. Used during the {@linkplain #read()
   * data reading phase}.
   *
   * <p>Implementors should not care about {@code maxRecords} and {@code skipRecords}, these will be
   * applied later on, see {@link #applyPerFileLimits(Flux)}.
   *
   * @param url The URL to read; must not be null; must be accessible and readable (but not
   *     necessarily hosted on the local filesystem).
   * @return A stream of {@link Record}s; never null but may be empty.
   */
  @NonNull
  protected abstract Flux<Record> readSingleFile(@NonNull URL url);

  /**
   * Returns a new {@link RecordWriter} instance; cannot be null. Only used when writing. each
   * invocation of this method is expected to return a newly-allocated instance.
   */
  @NonNull
  protected abstract RecordWriter newSingleFileWriter();

  /**
   * A writer for {@link Record}s. Implementors are not expected to deal with thread-safety issues,
   * these are handled by this class.
   */
  protected interface RecordWriter extends AutoCloseable {

    /**
     * Writes the record to the destination file, in a thread-safe manner (i.e., this method is
     * never called concurrently).
     *
     * @param record The record to write.
     * @throws IOException If an I/O error occurs while writing the record or dealing with the file
     *     (open, close, etc.).
     */
    void write(@NonNull Record record) throws IOException;

    /**
     * Closes the underlying file being written. Once this method is called, it is guaranteed that
     * {@link #write(Record)} will not be called anymore.
     *
     * @throws IOException If an I/O error occurs while closing.
     */
    @Override
    void close() throws IOException;
  }

  /**
   * Validates and computes the list of URLs to be read or written. This method honors both the
   * {@link #URLFILE} and {@link #URL} configuration options.
   *
   * <p>Expected to be called during the {@linkplain #configure(Config, boolean) configuration
   * phase}.
   */
  @NonNull
  protected List<URL> loadURLs(@NonNull Config settings) {
    boolean hasUrl = ConfigUtils.isPathPresentAndNotEmpty(settings, URL);
    boolean hasUrlfile = ConfigUtils.isPathPresentAndNotEmpty(settings, URLFILE);
    if (read) {
      // for LOAD
      if (!hasUrl && !hasUrlfile) {
        throw new BulkConfigurationException(
            String.format(
                "A URL or URL file is mandatory when using the %s connector for LOAD. Please set connector.%s.url or connector.%s.urlfile "
                    + "and try again. See settings.md or help for more information.",
                getConnectorName(), getConnectorName(), getConnectorName()));
      }
      if (hasUrl && hasUrlfile) {
        LOGGER.debug("You specified both URL and URL file. The URL file will take precedence.");
      }
    } else {
      // for UNLOAD we are not supporting urlfile parameter
      if (hasUrlfile) {
        throw new BulkConfigurationException("The urlfile parameter is not supported for UNLOAD");
      }
      if (!hasUrl) {
        throw new BulkConfigurationException(
            String.format(
                "A URL is mandatory when using the %s connector for UNLOAD. Please set connector.%s.url "
                    + "and try again. See settings.md or help for more information.",
                getConnectorName(), getConnectorName()));
      }
    }
    if (hasUrlfile) {
      try {
        return ConfigUtils.getURLsFromFile(ConfigUtils.getPath(settings, URLFILE));
      } catch (IOException e) {
        throw new BulkConfigurationException(
            "Problem when retrieving urls from file specified by the URL file parameter", e);
      }
    } else {
      return Collections.singletonList(ConfigUtils.getURL(settings, URL));
    }
  }

  /**
   * Inspects the list or URLs as loaded by {@link #loadURLs(Config)} and determines exactly what
   * files and folders need to be read.
   *
   * <p>Should be called at the beginning of the {@linkplain #init() initialization process}, but
   * only when reading, never when writing.
   *
   * <p>This method expects that {@link #loadURLs(Config)} has been previously called.
   */
  protected void processURLsForRead() throws URISyntaxException, IOException {
    resourceCount = 0;
    for (URL u : urls) {
      try {
        Path root = Paths.get(u.toURI());
        if (Files.isDirectory(root)) {
          if (!Files.isReadable(root)) {
            throw new IllegalArgumentException(
                String.format("Directory is not readable: %s.", root));
          }
          roots.add(root);
          int inDirectoryResourceCount =
              Objects.requireNonNull(scanRootDirectory(root).take(100).count().block()).intValue();
          if (inDirectoryResourceCount == 0) {
            if (IOUtils.countReadableFiles(root, recursive) == 0) {
              LOGGER.warn("Directory {} has no readable files.", root);
            } else {
              LOGGER.warn(
                  "No files in directory {} matched the connector.{}.fileNamePattern of \"{}\".",
                  root,
                  getConnectorName(),
                  pattern);
            }
          }
          resourceCount += inDirectoryResourceCount;
        } else {
          resourceCount += 1;
          files.add(u);
        }
      } catch (FileSystemNotFoundException ignored) {
        // not a path on a known filesystem, fall back to reading from URL directly
        files.add(u);
        resourceCount++;
      }
    }
  }

  /**
   * Inspects the list or URLs as loaded by {@link #loadURLs(Config)} and determines if the
   * connector should write to a single file, or to a directory of files.
   *
   * <p>Should be called at the beginning of the {@linkplain #init() initialization process}, but
   * only when writing, never when reading.
   *
   * <p>This method expects that {@link #loadURLs(Config)} has been previously called, and also
   * expects exactly one URL to be present, which can be either a directory or a file.
   */
  protected void processURLsForWrite() throws URISyntaxException, IOException {
    try {
      resourceCount = -1;
      Path root = Paths.get(urls.get(0).toURI()); // for UNLOAD always one URL
      if (!Files.exists(root)) {
        root = Files.createDirectories(root);
      }
      if (Files.isDirectory(root)) {
        if (!Files.isWritable(root)) {
          throw new IllegalArgumentException(String.format("Directory is not writable: %s.", root));
        }
        if (IOUtils.isDirectoryNonEmpty(root)) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid value for connector.%s.url: target directory "
                      + root
                      + " must be empty.",
                  getConnectorName()));
        }
        this.roots.add(root);
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to writing to URL directly
    }
  }

  /**
   * Scans a directory for readable files and returns the files found as a stream. Only used when
   * reading, never when writing. Normally used as part of the actual {@linkplain #read() data
   * reading phase}.
   */
  @NonNull
  protected Flux<URL> scanRootDirectory(@NonNull Path root) {
    try {
      // this stream will be closed by the flux, do not add it to a try-with-resources block
      @SuppressWarnings("StreamResourceLeak")
      Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1);
      PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
      return Flux.fromStream(files)
          .filter(Files::isReadable)
          .filter(Files::isRegularFile)
          .filter(matcher::matches)
          .map(
              file -> {
                try {
                  return file.toUri().toURL();
                } catch (MalformedURLException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException("Error scanning directory " + root, e);
    }
  }

  /**
   * Applies per-file limits to a stream of records coming from {@link
   * #readSingleFile(java.net.URL)}.
   */
  @NonNull
  protected Flux<Record> applyPerFileLimits(@NonNull Flux<Record> records) {
    if (skipRecords > 0) {
      records = records.skip(skipRecords);
    }
    if (maxRecords != -1) {
      records = records.take(maxRecords);
    }
    return records;
  }

  /**
   * Creates a new writing function using the given {@link RecordWriter} to write records to a
   * single destination.
   *
   * @param writer The writer to use; never null.
   * @return a function to apply to a flux of records; the function is expected to write each
   *     record, then return the record untouched. I/O errors should be propagated downstream.
   */
  @NonNull
  protected Function<Flux<Record>, Flux<Record>> writeRecords(@NonNull RecordWriter writer) {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Record record = signal.get();
                    assert record != null;
                    try {
                      writer.write(record);
                    } catch (Exception e) {
                      // Note that we may be inside a parallel flux;
                      // sending more than one onError signal to downstream will result
                      // in all onError signals but the first to be dropped.
                      // The framework is expected to deal with that.
                      signal = Signal.error(e);
                    }
                  }
                  return signal;
                })
            .dematerialize();
  }

  /**
   * Returns the URL that the connector should write to. Not used for reads.
   *
   * <p>This can be either a single file or a directory of files. If the former, each invocation of
   * this method will return the same URL; if the latter, each invocation of this method will
   * generate a new URL inside the directory, with a unique file name.
   */
  @NonNull
  protected URL getOrCreateDestinationURL() {
    if (!roots.isEmpty()) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return roots.get(0).resolve(next).toUri().toURL(); // for UNLOAD always one URL
      } catch (MalformedURLException e) {
        throw new UncheckedIOException(
            String.format("Could not create file URL with format %s", fileNameFormat), e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return urls.get(0);
  }
}
