/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.commons;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class AbstractFileBasedConnector implements Connector {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileBasedConnector.class);
  public static final String URL = "url";
  public static final String URLFILE = "urlfile";
  public static final String FILE_NAME_PATTERN = "fileNamePattern";
  public static final String SKIP_RECORDS = "skipRecords";
  public static final String MAX_RECORDS = "maxRecords";
  public static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  public static final String RECURSIVE = "recursive";
  public static final String FILE_NAME_FORMAT = "fileNameFormat";

  protected List<URL> urls;
  protected boolean read;
  protected List<Path> roots;
  protected String fileNameFormat;
  protected List<URL> files;
  protected boolean recursive;
  protected String pattern;
  protected int resourceCount;
  protected Scheduler scheduler;
  protected int maxConcurrentFiles;
  protected List<ConnectorWriter> writers;
  @VisibleForTesting public AtomicInteger counter;

  @Override
  public Publisher<Record> read() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).flatMap(this::readURL),
        Flux.fromIterable(files).flatMap(this::readURL));
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).map(this::readURL),
        Flux.fromIterable(files).map(this::readURL));
  }

  @NonNull
  protected List<java.net.URL> loadURLs(LoaderConfig settings) {
    if (ConfigUtils.isPathPresentAndNotEmpty(settings, URLFILE)) {
      // suppress URL option
      try {
        return ConfigUtils.getURLsFromFile(settings.getPath(URLFILE));
      } catch (IOException e) {
        throw new BulkConfigurationException(
            "Problem when retrieving urls from file specified by the URL file parameter", e);
      }
    } else {
      return Collections.singletonList(settings.getURL(URL));
    }
  }

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  protected abstract Flux<Record> readURL(URL url);

  protected abstract String getConnectorName();

  protected void validateURL(LoaderConfig settings, boolean read) {
    if (read) {
      // for LOAD
      if (ConfigUtils.isPathAbsentOrEmpty(settings, URL)) {
        if (ConfigUtils.isPathAbsentOrEmpty(settings, URLFILE)) {
          throw new BulkConfigurationException(
              String.format(
                  "A URL or URL file is mandatory when using the %s connector for LOAD. Please set connector.%s.url or connector.%s.urlfile "
                      + "and try again. See settings.md or help for more information.",
                  getConnectorName(), getConnectorName(), getConnectorName()));
        }
      }
      if (ConfigUtils.isPathPresentAndNotEmpty(settings, URL)
          && ConfigUtils.isPathPresentAndNotEmpty(settings, URLFILE)) {
        LOGGER.debug("You specified both URL and URL file. The URL file will take precedence.");
      }
    } else {
      // for UNLOAD we are not supporting urlfile parameter
      if (ConfigUtils.isPathPresentAndNotEmpty(settings, URLFILE)) {
        throw new BulkConfigurationException("The urlfile parameter is not supported for UNLOAD");
      }
      if (ConfigUtils.isPathAbsentOrEmpty(settings, URL)) {
        throw new BulkConfigurationException(
            String.format(
                "A URL is mandatory when using the %s connector for UNLOAD. Please set connector.%s.url "
                    + "and try again. See settings.md or help for more information.",
                getConnectorName(), getConnectorName()));
      }
    }
  }

  protected void tryReadFromDirectories() throws URISyntaxException, IOException {
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

  protected Flux<URL> scanRootDirectory(Path root) {
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

  protected void tryWriteToDirectory() throws URISyntaxException, IOException {
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

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (writers != null) {
      IOException e = null;
      for (ConnectorWriter writer : writers) {
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

  protected Function<? super Publisher<Record>, ? extends Publisher<Record>> write(
      Supplier<ConnectorWriter> writerSupplier) {
    assert !read;
    writers = new CopyOnWriteArrayList<>();
    if (!roots.isEmpty() && maxConcurrentFiles > 1) {
      return upstream -> {
        ThreadFactory threadFactory = new DefaultThreadFactory(getConnectorName() + "-connector");
        scheduler = Schedulers.newParallel(maxConcurrentFiles, threadFactory);
        for (int i = 0; i < maxConcurrentFiles; i++) {
          writers.add(writerSupplier.get());
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
        ConnectorWriter writer = writerSupplier.get();
        writers.add(writer);
        return Flux.from(upstream).transform(writeRecords(writer));
      };
    }
  }

  private Function<Flux<Record>, Flux<Record>> writeRecords(ConnectorWriter writer) {
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

  @VisibleForTesting
  public URL getOrCreateDestinationURL() {
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
    return urls.get(0); // for UNLOAD always one URL
  }

  public interface ConnectorWriter {
    void write(Record record) throws IOException;

    void close() throws IOException;
  }
}
