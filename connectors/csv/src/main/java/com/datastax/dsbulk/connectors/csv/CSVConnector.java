/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultUnmappableRecord;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.WorkQueueProcessor;

/**
 * A connector for CSV files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link URLStreamHandler
 * handler} installed for it. For file URLs, it is also capable of reading several files at once
 * from a given root directory.
 *
 * <p>This connector is highly configurable; see its {@code reference.conf} file, bundled within its
 * jar archive, for detailed information.
 */
public class CSVConnector implements Connector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnector.class);

  private boolean read;
  private URL url;
  private Path root;
  private String pattern;
  private Charset encoding;
  private char delimiter;
  private char quote;
  private char escape;
  private char comment;
  private long skipLines;
  private long maxLines;
  private int maxConcurrentFiles;
  private boolean recursive;
  private boolean header;
  private String fileNameFormat;
  private long resourceCount;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  private AtomicInteger counter;
  private ExecutorService threadPool;
  private WorkQueueProcessor<Record> writeQueueProcessor;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    this.read = read;
    url = settings.getURL("url");
    pattern = settings.getString("fileNamePattern");
    encoding = settings.getCharset("encoding");
    delimiter = settings.getChar("delimiter");
    quote = settings.getChar("quote");
    escape = settings.getChar("escape");
    comment = settings.getChar("comment");
    skipLines = settings.getLong("skipLines");
    maxLines = settings.getLong("maxLines");
    maxConcurrentFiles = settings.getThreads("maxConcurrentFiles");
    recursive = settings.getBoolean("recursive");
    header = settings.getBoolean("header");
    fileNameFormat = settings.getString("fileNameFormat");
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      tryReadFromDirectory();
    } else {
      tryWriteToDirectory();
    }
    CsvFormat format = new CsvFormat();
    format.setDelimiter(delimiter);
    format.setQuote(quote);
    format.setQuoteEscape(escape);
    format.setComment(comment);
    if (read) {
      parserSettings = new CsvParserSettings();
      parserSettings.setFormat(format);
      // do not use this feature as the parser throws an error if the file
      // has fewer lines than skipLines;
      // we'll use the skip() operator instead.
      // parserSettings.setNumberOfRowsToSkip(skipLines);
      parserSettings.setHeaderExtractionEnabled(header);
      parserSettings.setLineSeparatorDetectionEnabled(true);
      parserSettings.setMaxCharsPerColumn(-1);
    } else {
      writerSettings = new CsvWriterSettings();
      writerSettings.setFormat(format);
      writerSettings.setQuoteEscapingEnabled(true);
      counter = new AtomicInteger(0);
    }
  }

  @Override
  public void validate(LoaderConfig settings, boolean read) throws BulkConfigurationException {
    try {
      if (!settings.hasPath("url")) {
        throw new BulkConfigurationException(
            "url is mandatory when using the csv connector. Please set connector.csv.url "
                + "and try again. See settings.md or help for more information.",
            "connector.csv");
      }
      settings.getURL("url");
      settings.getString("fileNamePattern");
      settings.getCharset("encoding");
      settings.getChar("delimiter");
      settings.getChar("quote");
      settings.getChar("escape");
      settings.getChar("comment");
      settings.getLong("skipLines");
      settings.getLong("maxLines");
      settings.getThreads("maxConcurrentFiles");
      settings.getBoolean("recursive");
      settings.getBoolean("header");
      settings.getString("fileNameFormat");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.csv");
    }
  }

  @Override
  public void close() throws Exception {
    if (writeQueueProcessor != null) {
      writeQueueProcessor.awaitAndShutdown(5, SECONDS);
    }
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool.awaitTermination(5, SECONDS);
      threadPool.shutdownNow();
    }
  }

  @Override
  public long estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public Publisher<Record> read() {
    assert read;
    if (root != null) {
      return scanRootDirectory().flatMap(this::readURL);
    } else {
      return readURL(url);
    }
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    if (root != null) {
      return scanRootDirectory().map(this::readURL);
    } else {
      return Flux.just(readURL(url));
    }
  }

  @Override
  public Subscriber<Record> write() {
    assert !read;
    if (root != null && maxConcurrentFiles > 1) {
      return multipleWriteSubscriber();
    } else {
      return singleWriteSubscriber();
    }
  }

  private void tryReadFromDirectory() throws URISyntaxException {
    try {
      resourceCount = 1L;
      Path root = Paths.get(url.toURI());
      if (Files.isDirectory(root)) {
        if (!Files.isReadable(root)) {
          throw new IllegalArgumentException("Directory is not readable: " + root);
        }
        this.root = root;
        resourceCount = scanRootDirectory().take(100).count().block();
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to reading from URL directly
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
    try {
      resourceCount = -1L;
      Path root = Paths.get(url.toURI());
      if (!Files.exists(root)) {
        root = Files.createDirectories(root);
      }
      if (Files.isDirectory(root)) {
        if (!Files.isWritable(root)) {
          throw new IllegalArgumentException("Directory is not writable: " + root);
        }
        this.root = root;
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to writing to URL directly
    }
  }

  private Flux<Record> readURL(URL url) {
    Flux<Record> records =
        Flux.create(
            sink -> {
              SimpleBackpressureController controller = new SimpleBackpressureController();
              sink.onRequest(controller::signalRequested);
              CsvParser parser = new CsvParser(parserSettings);
              LOGGER.debug("Reading {}", url);
              try (InputStream is = IOUtils.openInputStream(url)) {
                parser.beginParsing(is, encoding);
                URI resource = URIUtils.createResourceURI(url);
                while (true) {
                  if (sink.isCancelled()) {
                    break;
                  }
                  com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
                  ParsingContext context = parser.getContext();
                  String source = context.currentParsedContent();
                  if (row == null) {
                    break;
                  }
                  Record record;
                  long line = context.currentLine();
                  Supplier<URI> location =
                      Suppliers.memoize(() -> URIUtils.createLocationURI(url, line));
                  try {
                    if (header) {
                      record =
                          new DefaultRecord(
                              source,
                              () -> resource,
                              line,
                              location,
                              context.parsedHeaders(),
                              (Object[]) row.getValues());
                    } else {
                      record =
                          new DefaultRecord(
                              source, () -> resource, line, location, (Object[]) row.getValues());
                    }
                  } catch (Exception e) {
                    record = new DefaultUnmappableRecord(source, () -> resource, line, location, e);
                  }
                  LOGGER.trace("Emitting record {}", record);
                  controller.awaitRequested(1);
                  sink.next(record);
                }
                LOGGER.debug("Done reading {}", url);
                sink.complete();
              } catch (Exception e) {
                LOGGER.error("Error reading from " + url, e);
                sink.error(e);
              }
            },
            FluxSink.OverflowStrategy.ERROR);
    if (skipLines > 0) {
      records = records.skip(skipLines);
    }
    if (maxLines != -1) {
      records = records.take(maxLines);
    }
    return records;
  }

  private Flux<URL> scanRootDirectory() {
    PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
    return Flux.defer(
            () -> {
              try {
                // this stream will be closed by the flux, do not add it to a try-with-resources block
                Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1);
                return Flux.fromStream(files);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .filter(Files::isReadable)
        .filter(Files::isRegularFile)
        .filter(matcher::matches)
        .map(
            file -> {
              try {
                return file.toUri().toURL();
              } catch (MalformedURLException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @NotNull
  private Subscriber<Record> singleWriteSubscriber() {

    return new BaseSubscriber<Record>() {

      private URL url;
      private CsvWriter writer;

      private void start() {
        url = getOrCreateDestinationURL();
        writer = createCSVWriter(url);
        LOGGER.debug("Writing " + url);
      }

      @Override
      protected void hookOnSubscribe(Subscription subscription) {
        start();
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      protected void hookOnNext(Record record) {
        if (root != null && writer.getRecordCount() == maxLines) {
          end();
          start();
        }
        if (header && writer.getRecordCount() == 0) {
          writer.writeHeaders(record.fields());
        }
        LOGGER.trace("Writing record {}", record);
        writer.writeRow(record.values());
      }

      @Override
      protected void hookOnError(Throwable t) {
        LOGGER.error("Error writing to " + url, t);
      }

      @Override
      protected void hookFinally(SignalType type) {
        end();
      }

      private void end() {
        LOGGER.debug("Done writing {}", url);
        if (writer != null) {
          try {
            writer.close();
          } catch (IllegalStateException e) {
            Throwable root = Throwables.getRootCause(e);
            if (root instanceof ClosedByInterruptException) {
              // ok, happens when the workflow has been interrupted
            } else {
              LOGGER.error("Could not close " + url, root);
            }
          }
        }
      }
    };
  }

  private Subscriber<Record> multipleWriteSubscriber() {
    threadPool =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("csv-connector-%d").build());
    writeQueueProcessor = WorkQueueProcessor.<Record>builder().executor(threadPool).build();
    for (int i = 0; i < maxConcurrentFiles; i++) {
      writeQueueProcessor.subscribe(singleWriteSubscriber());
    }
    return writeQueueProcessor;
  }

  private CsvWriter createCSVWriter(URL url) {
    try {
      return new CsvWriter(
          new OutputStreamWriter(IOUtils.openOutputStream(url), encoding), writerSettings);
    } catch (Exception e) {
      LOGGER.error("Could not create CSV writer for " + url, e);
      throw new RuntimeException(e);
    }
  }

  private URL getOrCreateDestinationURL() {
    if (root != null) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return root.resolve(next).toUri().toURL();
      } catch (Exception e) {
        LOGGER.error("Could not create file URL with format " + fileNameFormat, e);
        throw new RuntimeException(e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return url;
  }
}
