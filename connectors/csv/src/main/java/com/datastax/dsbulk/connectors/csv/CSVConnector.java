/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.csv.internal.CSVRecordPublisher;
import com.datastax.dsbulk.connectors.csv.internal.CSVRecordSubscriber;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
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
  private String fileNameFormat;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  private AtomicInteger counter;
  private ExecutorService threadPool;

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
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.csv");
    }
  }

  @Override
  public void close() throws Exception {
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool.awaitTermination(1, MINUTES);
      threadPool.shutdownNow();
    }
  }

  @Override
  public Publisher<Record> read() {
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

  private Flux<Record> readSingleFile(URL url) {
    CsvParser parser = new CsvParser(parserSettings);
    Flux<Record> records = Flux.from(new CSVRecordPublisher(parser, url, encoding, header));
    if (skipLines > 0) {
      records = records.skip(skipLines);
    }
    if (maxLines != -1) {
      records = records.take(maxLines);
    }
    return records;
  }

  private Publisher<Record> readMultipleFiles() {
    Scheduler scheduler =
        maxThreads > 1 ? Schedulers.fromExecutor(threadPool) : Schedulers.immediate();
    PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
    return Flux.defer(
            () -> {
              try {
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
            })
        .flatMap(url -> readSingleFile(url).subscribeOn(scheduler), maxThreads);
  }

  @NotNull
  private Subscriber<Record> writeSingleThread() {
    return new CSVRecordSubscriber(
        writerSettings, url, root, fileNameFormat, counter, header, maxLines);
  }

  private Subscriber<Record> writeMultipleThreads() {
    WorkQueueProcessor<Record> dispatcher =
        WorkQueueProcessor.<Record>builder().executor(threadPool).build();
    for (int i = 0; i < maxThreads; i++) {
      dispatcher.subscribe(writeSingleThread());
    }
    return dispatcher;
  }

  public static URI getCurrentLocation(URL url, ParsingContext context) {
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

  public static BufferedInputStream openInputStream(URL url) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream
        ? ((BufferedInputStream) in)
        : new BufferedInputStream(in);
  }

  public static BufferedOutputStream openOutputStream(URL url)
      throws IOException, URISyntaxException {
    OutputStream out;
    // file URLs do not support writing, only reading,
    // so we need to special-case them here
    if (url.getProtocol().equals("file")) {
      out = new FileOutputStream(new File(url.toURI()));
    } else {
      URLConnection connection = url.openConnection();
      connection.setDoOutput(true);
      out = connection.getOutputStream();
    }
    return out instanceof BufferedOutputStream
        ? ((BufferedOutputStream) out)
        : new BufferedOutputStream(out);
  }
}
