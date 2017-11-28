/*
 * Copyright DataStax Inc.
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
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultUnmappableRecord;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.IOException;
import java.io.Reader;
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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
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
  private static final String URL = "url";
  private static final String FILE_NAME_PATTERN = "fileNamePattern";
  private static final String ENCODING = "encoding";
  private static final String DELIMITER = "delimiter";
  private static final String QUOTE = "quote";
  private static final String ESCAPE = "escape";
  private static final String COMMENT = "comment";
  private static final String SKIP_LINES = "skipLines";
  private static final String MAX_LINES = "maxLines";
  private static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  private static final String RECURSIVE = "recursive";
  private static final String HEADER = "header";
  private static final String FILE_NAME_FORMAT = "fileNameFormat";
  private static final String MAX_CHARS_PER_COLUMN = "maxCharsPerColumn";

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
  private int resourceCount;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  private AtomicInteger counter;
  private ExecutorService threadPool;
  private WorkQueueProcessor<Record> writeQueueProcessor;
  private int maxCharsPerColumn;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      if (!settings.hasPath("url")) {
        throw new BulkConfigurationException(
            "url is mandatory when using the csv connector. Please set connector.csv.url "
                + "and try again. See settings.md or help for more information.",
            "connector.csv");
      }
      this.read = read;
      url = settings.getURL(URL);
      pattern = settings.getString(FILE_NAME_PATTERN);
      encoding = settings.getCharset(ENCODING);
      delimiter = settings.getChar(DELIMITER);
      quote = settings.getChar(QUOTE);
      escape = settings.getChar(ESCAPE);
      comment = settings.getChar(COMMENT);
      skipLines = settings.getLong(SKIP_LINES);
      maxLines = settings.getLong(MAX_LINES);
      maxConcurrentFiles = settings.getThreads(MAX_CONCURRENT_FILES);
      recursive = settings.getBoolean(RECURSIVE);
      header = settings.getBoolean(HEADER);
      fileNameFormat = settings.getString(FILE_NAME_FORMAT);
      maxCharsPerColumn = settings.getInt(MAX_CHARS_PER_COLUMN);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.csv");
    }
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
      parserSettings.setMaxCharsPerColumn(maxCharsPerColumn);
    } else {
      writerSettings = new CsvWriterSettings();
      writerSettings.setFormat(format);
      writerSettings.setQuoteEscapingEnabled(true);
      counter = new AtomicInteger(0);
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
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public boolean isWriteToStandardOutput() {
    return url.getProtocol().equalsIgnoreCase(LoaderURLStreamHandlerFactory.STDOUT);
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
      resourceCount = 1;
      Path root = Paths.get(url.toURI());
      if (Files.isDirectory(root)) {
        if (!Files.isReadable(root)) {
          throw new IllegalArgumentException("Directory is not readable: " + root);
        }
        this.root = root;
        resourceCount = scanRootDirectory().take(100).count().block().intValue();
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to reading from URL directly
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
    try {
      resourceCount = -1;
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
              CsvParser parser = new CsvParser(parserSettings);
              SimpleBackpressureController controller = new SimpleBackpressureController();
              sink.onDispose(parser::stopParsing);
              sink.onRequest(controller::signalRequested);
              LOGGER.debug("Reading {}", url);
              try (Reader r = IOUtils.newBufferedReader(url, encoding)) {
                parser.beginParsing(r);
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
                      // also emit indexed fields
                      Streams.forEachPair(
                          IntStream.range(0, row.getValues().length).mapToObj(Integer::toString),
                          Arrays.stream(row.getValues()),
                          ((DefaultRecord) record)::setFieldValue);
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
                LOGGER.error(String.format("Error reading from %s: %s", url, e.getMessage()), e);
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
        LOGGER.error(String.format("Error writing to %s: %s", url, t.getMessage()), t);
      }

      @Override
      protected void hookOnComplete() {
        LOGGER.debug("Done writing {}", url);
      }

      @Override
      protected void hookFinally(SignalType type) {
        end();
      }

      private void end() {
        if (writer != null) {
          try {
            writer.close();
          } catch (IllegalStateException e) {
            Throwable root = Throwables.getRootCause(e);
            if (!(root instanceof ClosedByInterruptException)) {
              LOGGER.error(String.format("Could not close %s: %s", url, root.getMessage()), root);
            }
            // else ok, happens when the workflow has been interrupted
          }
        }
      }
    };
  }

  private Subscriber<Record> multipleWriteSubscriber() {
    threadPool =
        Executors.newFixedThreadPool(
            maxConcurrentFiles,
            new ThreadFactoryBuilder().setNameFormat("csv-connector-%d").build());
    writeQueueProcessor = WorkQueueProcessor.<Record>builder().executor(threadPool).build();
    for (int i = 0; i < maxConcurrentFiles; i++) {
      writeQueueProcessor.subscribe(singleWriteSubscriber());
    }
    return writeQueueProcessor;
  }

  private CsvWriter createCSVWriter(URL url) {
    try {
      return new CsvWriter(IOUtils.newBufferedWriter(url, encoding), writerSettings);
    } catch (Exception e) {
      LOGGER.error(String.format("Could not create CSV writer for %s: %s", url, e.getMessage()), e);
      throw new RuntimeException(e);
    }
  }

  private URL getOrCreateDestinationURL() {
    if (root != null) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return root.resolve(next).toUri().toURL();
      } catch (Exception e) {
        LOGGER.error(
            String.format(
                "Could not create file URL with format %s: %s", fileNameFormat, e.getMessage()),
            e);
        throw new RuntimeException(e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return url;
  }
}
