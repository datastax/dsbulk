/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.connectors.commons;

import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.io.CompressedIOUtils;
import com.datastax.oss.dsbulk.io.IOUtils;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
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
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

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
  private static final String STDIN_PROTOCOL = "std";

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
  protected int maxConcurrentFiles;
  protected Deque<RecordWriter> writers;
  protected RecordWriter singleWriter;
  protected AtomicInteger fileCounter;
  protected AtomicInteger nextWriterIndex;

  // Public API

  @Override
  public int readConcurrency() {
    assert read;
    return Math.min(resourceCount, maxConcurrentFiles);
  }

  @Override
  public int writeConcurrency() {
    assert !read;
    // When writing to an URL, force write concurrency to 1
    if (roots.isEmpty()) {
      return 1;
    }
    return maxConcurrentFiles;
  }

  @Override
  public void configure(@NonNull Config settings, boolean read) {
    this.read = read;
    urls = loadURLs(settings);
    encoding = ConfigUtils.getCharset(settings, ENCODING);
    compression = settings.getString(COMPRESSION);
    if (!CompressedIOUtils.isSupportedCompression(compression, read)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value for connector.csv.%s, valid values: %s, got: '%s'",
              COMPRESSION,
              String.join(",", CompressedIOUtils.getSupportedCompressions(read)),
              compression));
    }
    pattern = settings.getString(FILE_NAME_PATTERN);
    if (!CompressedIOUtils.isNoneCompression(compression)
        && ConfigUtils.hasReferenceValue(settings, FILE_NAME_PATTERN)) {
      pattern = pattern + CompressedIOUtils.getCompressionSuffix(compression);
    }
    fileNameFormat = settings.getString(FILE_NAME_FORMAT);
    if (!CompressedIOUtils.isNoneCompression(compression)
        && ConfigUtils.hasReferenceValue(settings, FILE_NAME_FORMAT)) {
      fileNameFormat = fileNameFormat + CompressedIOUtils.getCompressionSuffix(compression);
    }
    recursive = settings.getBoolean(RECURSIVE);
    if ("AUTO".equals(settings.getString(MAX_CONCURRENT_FILES))) {
      maxConcurrentFiles = ConfigUtils.resolveThreads(read ? "1C" : "0.5C");
    } else {
      maxConcurrentFiles = ConfigUtils.getThreads(settings, MAX_CONCURRENT_FILES);
    }
    skipRecords = settings.getLong(SKIP_RECORDS);
    maxRecords = settings.getLong(MAX_RECORDS);
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      processURLsForRead();
    } else {
      processURLsForWrite();
      fileCounter = new AtomicInteger(0);
      nextWriterIndex = new AtomicInteger(0);
      if (!roots.isEmpty() && maxConcurrentFiles > 1) {
        writers = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < maxConcurrentFiles; i++) {
          writers.add(newSingleFileWriter());
        }
      } else {
        singleWriter = newSingleFileWriter();
      }
    }
  }

  @NonNull
  @Override
  public Publisher<Publisher<Record>> read() {
    assert read;
    return Flux.concat(
            Flux.fromIterable(roots).flatMap(this::scanRootDirectory), Flux.fromIterable(files))
        .map(url -> readSingleFile(url).transform(this::applyPerFileLimits));
  }

  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  @NonNull
  @Override
  public Function<Publisher<Record>, Publisher<Record>> write() {
    assert !read;
    if (!roots.isEmpty() && maxConcurrentFiles > 1) {
      return records ->
          Flux.from(records)
              .concatMap(
                  record ->
                      Mono.subscriberContext()
                          .flatMap(
                              ctx -> {
                                try {
                                  RecordWriter writer = ctx.get("WRITER");
                                  writer.write(record);
                                  return Mono.just(record);
                                } catch (Exception e) {
                                  return Mono.error(e);
                                }
                              }),
                  500)
              .concatWith(
                  Mono.subscriberContext()
                      .flatMap(
                          ctx -> {
                            try {
                              RecordWriter writer = ctx.get("WRITER");
                              writer.flush();
                              writers.offer(writer);
                              return Mono.empty();
                            } catch (Exception e) {
                              return Mono.error(e);
                            }
                          }))
              .subscriberContext(ctx -> ctx.put("WRITER", writers.remove()));
    } else {
      return records ->
          Flux.from(records)
              .concatMap(
                  record -> {
                    try {
                      singleWriter.write(record);
                      return Mono.just(record);
                    } catch (Exception e) {
                      return Mono.error(e);
                    }
                  },
                  500)
              .concatWith(
                  Flux.create(
                      sink -> {
                        try {
                          singleWriter.flush();
                          sink.complete();
                        } catch (Exception e) {
                          sink.error(e);
                        }
                      }));
    }
  }

  @Override
  public void close() {
    if (writers != null) {
      IOException e = null;
      for (RecordWriter writer : writers) {
        try {
          writer.flush();
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
    if (singleWriter != null) {
      try {
        singleWriter.flush();
        singleWriter.close();
      } catch (IOException e) {
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
  protected Flux<Record> readSingleFile(@NonNull URL url) {
    return Flux.generate(
        () -> newSingleFileReader(url),
        RecordReader::readNext,
        recordReader -> {
          try {
            recordReader.close();
          } catch (IOException e) {
            LOGGER.error("Error closing " + url, e);
          }
        });
  }

  /**
   * Returns a new {@link RecordReader} instance; cannot be null. Only used when reading. Each
   * invocation of this method is expected to return a newly-allocated instance. The reader is
   * expected to be initialized already, and ready to emit its first record. It is possible to throw
   * {@link IOException} if the reader cannot be initialized.
   */
  @NonNull
  protected abstract RecordReader newSingleFileReader(@NonNull URL url) throws IOException;

  /**
   * A reader for {@link Record}s. Implementors are not expected to deal with thread-safety issues,
   * these are handled by this class.
   */
  protected interface RecordReader extends AutoCloseable {

    /**
     * Reads the next record and emits the record to the sink, if any. It may also signal
     * completion, if there are no more records to be read, or an error if the next record cannot be
     * parsed.
     *
     * <p>This method must never throw; any error should be signaled to the sink.
     *
     * @param sink The {@link SynchronousSink} that will receive the parsed record, or a termination
     *     signal.
     * @return this reader.
     */
    @NonNull
    RecordReader readNext(@NonNull SynchronousSink<Record> sink);

    /**
     * Closes the underlying file being read. Once this method is called, it is guaranteed that
     * {@link #readNext(SynchronousSink)} will not be called anymore.
     *
     * @throws IOException If an I/O error occurs while closing.
     */
    @Override
    void close() throws IOException;
  }

  /**
   * Returns a new {@link RecordWriter} instance; cannot be null. Only used when writing. Each
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
     * Flushes all pending writes.
     *
     * @throws IOException If an I/O error occurs while flushing.
     */
    void flush() throws IOException;

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
        throw new IllegalArgumentException(
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
        throw new IllegalArgumentException("The urlfile parameter is not supported for UNLOAD");
      }
      if (!hasUrl) {
        throw new IllegalArgumentException(
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
        throw new IllegalArgumentException(
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
              Objects.requireNonNull(scanRootDirectory(root).take(1000).count().block()).intValue();
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
      @SuppressWarnings({"StreamResourceLeak", "BlockingMethodInNonBlockingContext"})
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
  @SuppressWarnings("ReactiveStreamsUnusedPublisher")
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
        String next = String.format(fileNameFormat, fileCounter.incrementAndGet());
        return roots.get(0).resolve(next).toUri().toURL(); // for UNLOAD always one URL
      } catch (MalformedURLException e) {
        throw new UncheckedIOException(
            String.format("Could not create file URL with format %s", fileNameFormat), e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return urls.get(0);
  }

  protected boolean isDataSizeSamplingAvailable() {
    return urls != null && urls.size() == 1 && urls.get(0).getProtocol().equals(STDIN_PROTOCOL);
  }
}
