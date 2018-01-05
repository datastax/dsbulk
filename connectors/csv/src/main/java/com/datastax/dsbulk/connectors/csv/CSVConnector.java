/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv;

import static com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory.STD;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.google.common.base.Suppliers;
import com.google.common.collect.Streams;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import stormpot.Allocator;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;

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
  private static final String SKIP_RECORDS = "skipRecords";
  private static final String MAX_RECORDS = "maxRecords";
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
  private long skipRecords;
  private long maxRecords;
  private int maxConcurrentFiles;
  private boolean recursive;
  private boolean header;
  private String fileNameFormat;
  private int maxCharsPerColumn;
  private int resourceCount;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  private AtomicInteger counter;
  private Scheduler scheduler;
  private BlazePool<PoolableCSVWriter> pool;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      if (!settings.hasPath(URL)) {
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
      skipRecords = settings.getLong(SKIP_RECORDS);
      maxRecords = settings.getLong(MAX_RECORDS);
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
      // has fewer lines than skipRecords;
      // we'll use the skip() operator instead.
      // parserSettings.setNumberOfRowsToSkip(skipRecords);
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
  public void close() throws InterruptedException {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (pool != null) {
      pool.shutdown().await(new Timeout(1, TimeUnit.MINUTES));
    }
  }

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public boolean isWriteToStandardOutput() {
    return url.getProtocol().equalsIgnoreCase(STD) && !read;
  }

  @Override
  public Supplier<? extends Publisher<Record>> read() {
    assert read;
    if (root != null) {
      return () -> scanRootDirectory().flatMap(this::readURL);
    } else {
      return () -> readURL(url);
    }
  }

  @Override
  public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
    if (root != null) {
      return () -> scanRootDirectory().map(this::readURL);
    } else {
      return () -> Flux.just(readURL(url));
    }
  }

  @Override
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    assert !read;
    if (root != null && maxConcurrentFiles > 1) {
      return upstream -> {
        scheduler = Schedulers.newParallel("csv-connector", maxConcurrentFiles);
        Config<PoolableCSVWriter> config =
            new Config<PoolableCSVWriter>()
                .setSize(maxConcurrentFiles)
                .setAllocator(new CSVWriterAllocator());
        pool = new BlazePool<>(config);
        Timeout timeout = new Timeout(Long.MAX_VALUE, TimeUnit.SECONDS);
        return Flux.from(upstream)
            .window(Queues.SMALL_BUFFER_SIZE)
            .parallel(maxConcurrentFiles)
            .runOn(scheduler)
            .flatMap(
                records -> {
                  try {
                    PoolableCSVWriter writer = pool.claim(timeout);
                    if (writer != null) {
                      try {
                        return records.transform(writeRecords(writer));
                      } finally {
                        writer.release();
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return Flux.empty();
                })
            .sequential();
      };
    } else {
      return upstream -> {
        PoolableCSVWriter writer = new PoolableCSVWriter(null);
        return Flux.from(upstream).transform(writeRecords(writer)).doOnTerminate(writer::close);
      };
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
        if (IOUtils.isDirectoryNonEmpty(root)) {
          throw new IllegalArgumentException(
              "connector.csv.url target directory: " + root + " must be empty.");
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
              // DAT-177: Do not call sink.onDispose nor sink.onCancel,
              // as doing so seems to prevent the flow from completing in rare occasions.
              sink.onRequest(controller::signalRequested);
              LOGGER.debug("Reading {}", url);
              try (Reader r = IOUtils.newBufferedReader(url, encoding)) {
                parser.beginParsing(r);
                URI resource = URIUtils.createResourceURI(url);
                long recordNumber = 1;
                while (!sink.isCancelled()) {
                  com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
                  ParsingContext context = parser.getContext();
                  String source = context.currentParsedContent();
                  if (row == null) {
                    break;
                  }
                  Record record;
                  long finalRecordNumber = recordNumber++;
                  Supplier<URI> location =
                      Suppliers.memoize(() -> URIUtils.createLocationURI(url, finalRecordNumber));
                  try {
                    if (header) {
                      record =
                          new DefaultRecord(
                              source,
                              () -> resource,
                              finalRecordNumber,
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
                              source,
                              () -> resource,
                              finalRecordNumber,
                              location,
                              (Object[]) row.getValues());
                    }
                  } catch (Exception e) {
                    record =
                        new DefaultErrorRecord(
                            source, () -> resource, finalRecordNumber, location, e);
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
    if (skipRecords > 0) {
      records = records.skip(skipRecords);
    }
    if (maxRecords != -1) {
      records = records.take(maxRecords);
    }
    return records;
  }

  private Flux<URL> scanRootDirectory() {
    PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
    return Flux.defer(
            () -> {
              try {
                // this stream will be closed by the flux, do not add it to a try-with-resources
                // block
                Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1);
                return Flux.fromStream(files);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
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
                throw new UncheckedIOException(e);
              }
            });
  }

  private Function<Flux<Record>, Flux<Record>> writeRecords(PoolableCSVWriter writer) {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    try {
                      writer.write(signal.get());
                    } catch (Exception e) {
                      signal = Signal.error(e);
                    }
                  }
                  return signal;
                })
            .dematerialize();
  }

  private class CSVWriterAllocator implements Allocator<PoolableCSVWriter> {
    @Override
    public PoolableCSVWriter allocate(Slot slot) {
      return new PoolableCSVWriter(slot);
    }

    @Override
    public void deallocate(PoolableCSVWriter writer) {
      writer.close();
    }
  }

  private class PoolableCSVWriter implements Poolable {

    private final Slot slot;

    private URL url;
    private CsvWriter writer;

    private PoolableCSVWriter(Slot slot) {
      this.slot = slot;
    }

    @Override
    public void release() {
      slot.release(this);
    }

    private void write(Record record) {
      try {
        if (writer == null) {
          open();
        } else if (shouldRoll()) {
          close();
          open();
        }
        if (shouldWriteHeader()) {
          writer.writeHeaders(record.fields());
        }
        LOGGER.trace("Writing record {} to {}", record, url);
        writer.writeRow(record.values());
      } catch (IOException e) {
        throw new UncheckedIOException(
            String.format("Error writing to %s: %s", url, e.getMessage()), e);
      }
    }

    private boolean shouldWriteHeader() {
      return header && writer.getRecordCount() == 0;
    }

    private boolean shouldRoll() {
      return root != null && writer.getRecordCount() == maxRecords;
    }

    private void open() throws IOException {
      url = getOrCreateDestinationURL();
      writer = new CsvWriter(IOUtils.newBufferedWriter(url, encoding), writerSettings);
      LOGGER.debug("Writing {}", url);
    }

    private void close() {
      if (writer != null) {
        try {
          writer.close();
          LOGGER.debug("Done writing {}", url);
          writer = null;
        } catch (Exception e) {
          throw new UncheckedIOException(
              new IOException(String.format("Error closing %s: %s", url, e.getMessage()), e));
        }
      }
    }
  }

  private URL getOrCreateDestinationURL() {
    if (root != null) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return root.resolve(next).toUri().toURL();
      } catch (MalformedURLException e) {
        throw new UncheckedIOException(
            String.format(
                "Could not create file URL with format %s: %s", fileNameFormat, e.getMessage()),
            e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return url;
  }
}
