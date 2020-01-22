/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getURLsFromFile;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathAbsentOrEmpty;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathPresentAndNotEmpty;
import static com.datastax.dsbulk.commons.internal.io.IOUtils.countReadableFiles;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
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
import java.util.function.Function;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;

public abstract class AbstractConnector implements Connector {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnector.class);
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
  protected List<URL> files;
  protected boolean recursive;
  protected String pattern;
  protected int resourceCount;
  protected Scheduler scheduler;
  protected List<ConnectorWriter> writers;

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
    if (isPathPresentAndNotEmpty(settings, URLFILE)) {
      // suppress URL option
      try {
        return getURLsFromFile(settings.getPath(URLFILE));
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

  public abstract Flux<Record> readURL(URL url);

  protected void validateURL(LoaderConfig settings, boolean read) {
    if (read) {
      // for LOAD
      if (isPathAbsentOrEmpty(settings, URL)) {
        if (isPathAbsentOrEmpty(settings, URLFILE)) {
          throw new BulkConfigurationException(
              "A URL or URL file is mandatory when using the json connector for LOAD. Please set connector.json.url or connector.json.urlfile "
                  + "and try again. See settings.md or help for more information.");
        }
      }
      if (isPathPresentAndNotEmpty(settings, URL) && isPathPresentAndNotEmpty(settings, URLFILE)) {
        LOGGER.debug("You specified both URL and URL file. The URL file will take precedence.");
      }
    } else {
      // for UNLOAD we are not supporting urlfile parameter
      if (isPathPresentAndNotEmpty(settings, URLFILE)) {
        throw new BulkConfigurationException("The urlfile parameter is not supported for UNLOAD");
      }
      if (isPathAbsentOrEmpty(settings, URL)) {
        throw new BulkConfigurationException(
            "A URL is mandatory when using the json connector for UNLOAD. Please set connector.json.url "
                + "and try again. See settings.md or help for more information.");
      }
    }
  }

  protected Function<Flux<Record>, Flux<Record>> writeRecords(ConnectorWriter writer) {
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
                      // Note that we may be are inside a parallel flux;
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
            if (countReadableFiles(root, recursive) == 0) {
              LOGGER.warn("Directory {} has no readable files.", root);
            } else {
              LOGGER.warn(
                  "No files in directory {} matched the connector.json.fileNamePattern of \"{}\".",
                  root,
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
              "Invalid value for connector.json.url: target directory " + root + " must be empty.");
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
}
