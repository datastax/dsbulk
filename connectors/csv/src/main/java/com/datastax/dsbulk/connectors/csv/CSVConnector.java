/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.io.FileSplitter;
import com.datastax.dsbulk.commons.url.URLUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A connector for CSV files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link
 * java.net.URLStreamHandler handler} installed for it. For file URLs, it is also capable of reading
 * several files at once from a given root directory.
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
  private int maxThreads;
  private boolean recursive;
  private boolean header;
  private boolean mappedFilesEnabled;
  private long mappedFileChunkSize;
  private String fileNameFormat;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  private AtomicInteger counter;
  private ExecutorService threadPool;
  private Scheduler scheduler;

  @Override
  public void configure(LoaderConfig settings, boolean read) throws MalformedURLException {
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
    maxThreads = settings.getThreads("maxThreads");
    recursive = settings.getBoolean("recursive");
    header = settings.getBoolean("header");
    fileNameFormat = settings.getString("fileNameFormat");
    mappedFilesEnabled = settings.getBoolean("mappedFilesEnabled");
    mappedFileChunkSize = settings.getMemorySize("mappedFileChunkSize").toBytes();
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
      parserSettings.setNumberOfRowsToSkip(skipLines);
      parserSettings.setHeaderExtractionEnabled(header);
      parserSettings.setLineSeparatorDetectionEnabled(true);
    } else {
      writerSettings = new CsvWriterSettings();
      writerSettings.setFormat(format);
      writerSettings.setQuoteEscapingEnabled(true);
      counter = new AtomicInteger(0);
    }
    if (maxThreads > 1) {
      threadPool =
          Executors.newFixedThreadPool(
              maxThreads, new ThreadFactoryBuilder().setNameFormat("csv-connector-%d").build());
      scheduler = Schedulers.fromExecutor(threadPool);
    } else {
      scheduler = Schedulers.immediate();
    }
  }

  @Override
  public void validate(LoaderConfig settings, boolean read) throws BulkConfigurationException {
    try {
      if (!settings.hasPath("url")) {
        throw new BulkConfigurationException(
            "url is mandatory when using the csv connector. Please set connector.csv.url "
                + "and try again. See settings.md or help for more information.");
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
      settings.getThreads("maxThreads");
      settings.getBoolean("recursive");
      settings.getBoolean("header");
      settings.getString("fileNameFormat");
      settings.getBoolean("mappedFilesEnabled");
      settings.getMemorySize("mappedFileChunkSize");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.csv");
    }
  }

  @Override
  public void close() throws Exception {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool.awaitTermination(1, MINUTES);
      threadPool.shutdownNow();
    }
  }

  @Override
  public Publisher<Record> read() throws URISyntaxException, IOException {
    assert read;
    if (root != null) {
      return readMultipleFiles();
    } else {
      return readSingleFile(url);
    }
  }

  @Override
  public Subscriber<Record> write() {
    assert !read;
    if (root != null && maxThreads > 1) {
      return writeMultipleThreads();
    } else {
      return writeSingleThread();
    }
  }

  private void tryReadFromDirectory() throws URISyntaxException {
    try {
      Path root = Paths.get(url.toURI());
      if (Files.isDirectory(root)) {
        if (!Files.isReadable(root)) {
          throw new IllegalArgumentException("Directory is not readable: " + root);
        }
        this.root = root;
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to reading from URL directly
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
    try {
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

  private Flux<Record> readSingleFile(URL url) throws URISyntaxException, IOException {
    Flux<Record> records = null;
    if (mappedFilesEnabled && maxThreads > 1 && url.getProtocol().equals("file")) {
      File file = new File(url.toURI());
      long fileLength = file.length();
      if (fileLength > mappedFileChunkSize) {
        records =
            splitInChunks(file).flatMap(u -> readURL(u).subscribeOn(scheduler), maxThreads, 1024);
      }
    }
    if (records == null) {
      records = readURL(url);
    }
    if (maxLines != -1) {
      records = records.take(maxLines);
    }
    return records;
  }

  private Flux<URL> splitInChunks(File file) throws IOException, URISyntaxException {
    FileSplitter splitter =
        new FileSplitter(file, encoding, header, skipLines, mappedFileChunkSize);
    return Flux.fromIterable(splitter.split());
  }

  private Flux<Record> readURL(URL url) {
    return Flux.create(
        e -> {
          CsvParser parser = new CsvParser(parserSettings);
          LOGGER.debug("Reading {}", url);
          try (InputStream is = URLUtils.openInputStream(url)) {
            parser.beginParsing(is, encoding);
            while (true) {
              com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
              ParsingContext context = parser.getContext();
              String source = context.currentParsedContent();
              if (row == null) {
                break;
              }
              if (e.isCancelled()) {
                break;
              }
              Record record;
              if (header) {
                record =
                    new DefaultRecord(
                        source,
                        Suppliers.memoize(() -> getCurrentLocation(url, context)),
                        context.headers(),
                        (Object[]) row.getValues());
              } else {
                record =
                    new DefaultRecord(
                        source,
                        Suppliers.memoize(() -> getCurrentLocation(url, context)),
                        (Object[]) row.getValues());
              }
              LOGGER.trace("Emitting record {}", record);
              e.next(record);
            }
            LOGGER.debug("Done reading {}", url);
            e.complete();
            parser.stopParsing();
          } catch (IOException e1) {
            LOGGER.error("Error writing to " + url, e1);
            e.error(e1);
            parser.stopParsing();
          }
        },
        FluxSink.OverflowStrategy.BUFFER);
  }

  private Publisher<Record> readMultipleFiles() {
    return scanRootDirectory()
        .flatMap(
            p -> {
              try {
                return readSingleFile(p.toUri().toURL()).subscribeOn(scheduler);
              } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
              }
            },
            maxThreads);
  }

  private Flux<Path> scanRootDirectory() {
    return Flux.create(
        e -> {
          PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
          try {
            Files.walkFileTree(
                root,
                Collections.emptySet(),
                recursive ? Integer.MAX_VALUE : 1,
                new SimpleFileVisitor<Path>() {

                  @Override
                  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                      throws IOException {
                    return e.isCancelled() ? TERMINATE : CONTINUE;
                  }

                  @Override
                  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                      throws IOException {
                    if (Files.isReadable(file)
                        && Files.isRegularFile(file)
                        && matcher.matches(file)
                        && !e.isCancelled()) {
                      e.next(file);
                    }
                    return e.isCancelled() ? TERMINATE : CONTINUE;
                  }

                  @Override
                  public FileVisitResult visitFileFailed(Path file, IOException ex)
                      throws IOException {
                    LOGGER.warn("Could not read " + file.toAbsolutePath().toUri().toURL(), e);
                    return e.isCancelled() ? TERMINATE : CONTINUE;
                  }
                });
            e.complete();
          } catch (IOException e1) {
            e.error(e1);
          }
        },
        FluxSink.OverflowStrategy.BUFFER);
  }

  @NotNull
  private Subscriber<Record> writeSingleThread() {

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
          writer.close();
        }
      }
    };
  }

  private Subscriber<Record> writeMultipleThreads() {
    WorkQueueProcessor<Record> dispatcher = WorkQueueProcessor.create(threadPool, 1024);
    for (int i = 0; i < maxThreads; i++) {
      dispatcher.subscribe(writeSingleThread());
    }
    return dispatcher;
  }

  private CsvWriter createCSVWriter(URL url) {
    try {
      return new CsvWriter(URLUtils.openOutputStream(url), writerSettings);
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

  private static URI getCurrentLocation(URL url, ParsingContext context) {
    long line = context.currentLine();
    int column = context.currentColumn();
    return URI.create(
        url.toExternalForm()
            + (url.getQuery() == null ? '?' : '&')
            + "?line="
            + line
            + "&column="
            + column);
  }
}
