/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.csv;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getURLsFromFile;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathAbsentOrEmpty;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathPresentAndNotEmpty;
import static com.datastax.dsbulk.commons.internal.io.IOUtils.countReadableFiles;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.MappedField;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultIndexedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.TextWritingException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import io.netty.util.concurrent.DefaultThreadFactory;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
  private static final TypeToken<String> STRING_TYPE_TOKEN = TypeToken.of(String.class);
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private static final String URL = "url";
  private static final String URLFILE = "urlfile";
  private static final String FILE_NAME_PATTERN = "fileNamePattern";
  private static final String ENCODING = "encoding";
  private static final String DELIMITER = "delimiter";
  private static final String QUOTE = "quote";
  private static final String ESCAPE = "escape";
  private static final String COMMENT = "comment";
  private static final String NEWLINE = "newline";
  private static final String SKIP_RECORDS = "skipRecords";
  private static final String MAX_RECORDS = "maxRecords";
  private static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  private static final String RECURSIVE = "recursive";
  private static final String HEADER = "header";
  private static final String FILE_NAME_FORMAT = "fileNameFormat";
  private static final String MAX_CHARS_PER_COLUMN = "maxCharsPerColumn";
  private static final String MAX_COLUMNS = "maxColumns";
  private static final String AUTO_NEWLINE = "auto";
  private static final String IGNORE_LEADING_WHITESPACES = "ignoreLeadingWhitespaces";
  private static final String IGNORE_TRAILING_WHITESPACES = "ignoreTrailingWhitespaces";
  private static final String IGNORE_LEADING_WHITESPACES_IN_QUOTES =
      "ignoreLeadingWhitespacesInQuotes";
  private static final String IGNORE_TRAILING_WHITESPACES_IN_QUOTES =
      "ignoreTrailingWhitespacesInQuotes";
  private static final String NORMALIZE_LINE_ENDINGS_IN_QUOTES = "normalizeLineEndingsInQuotes";
  private static final String NULL_VALUE = "nullValue";
  private static final String EMPTY_VALUE = "emptyValue";
  private static final String FLUSH_WINDOW = "flushWindow";

  private boolean read;
  private List<URL> urls;
  private List<Path> roots;
  private List<URL> files;
  private String pattern;
  private Charset encoding;
  private char delimiter;
  private char quote;
  private char escape;
  private char comment;
  private String newline;
  private long skipRecords;
  private long maxRecords;
  private int maxConcurrentFiles;
  private boolean recursive;
  private boolean header;
  private String fileNameFormat;
  private int maxCharsPerColumn;
  private int maxColumns;
  private boolean ignoreLeadingWhitespaces;
  private boolean ignoreTrailingWhitespaces;
  private boolean ignoreTrailingWhitespacesInQuotes;
  private boolean ignoreLeadingWhitespacesInQuotes;
  private boolean normalizeLineEndingsInQuotes;
  private String nullValue;
  private String emptyValue;
  private int flushWindow;
  private int resourceCount;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;
  @VisibleForTesting
  AtomicInteger counter;
  private Scheduler scheduler;
  private List<CSVWriter> writers;
  private boolean atLeastOneUrlWasLoadedSuccessfully = false;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      validateURL(settings, read);
      this.read = read;
      urls = loadURLs(settings);
      roots = new ArrayList<>();
      files = new ArrayList<>();
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
      maxColumns = settings.getInt(MAX_COLUMNS);
      newline = settings.getString(NEWLINE);
      ignoreLeadingWhitespaces = settings.getBoolean(IGNORE_LEADING_WHITESPACES);
      ignoreTrailingWhitespaces = settings.getBoolean(IGNORE_TRAILING_WHITESPACES);
      ignoreTrailingWhitespacesInQuotes = settings.getBoolean(IGNORE_LEADING_WHITESPACES_IN_QUOTES);
      ignoreLeadingWhitespacesInQuotes = settings.getBoolean(IGNORE_TRAILING_WHITESPACES_IN_QUOTES);
      normalizeLineEndingsInQuotes = settings.getBoolean(NORMALIZE_LINE_ENDINGS_IN_QUOTES);
      nullValue = settings.getIsNull(NULL_VALUE) ? null : settings.getString(NULL_VALUE);
      emptyValue = settings.getIsNull(EMPTY_VALUE) ? null : settings.getString(EMPTY_VALUE);
      flushWindow = settings.getInt(FLUSH_WINDOW);
      if (flushWindow < 1) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for connector.csv.%s: Expecting integer > 0, got: %d",
                FLUSH_WINDOW, flushWindow));
      }
      if (!AUTO_NEWLINE.equalsIgnoreCase(newline) && (newline.isEmpty() || newline.length() > 2)) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for connector.csv.%s: Expecting '%s' or a string containing 1 or 2 chars, got: '%s'",
                NEWLINE, AUTO_NEWLINE, newline));
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.csv");
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      tryReadFromDirectories();
    } else {
      tryWriteToDirectory();
    }
    CsvFormat format = new CsvFormat();
    format.setDelimiter(delimiter);
    format.setQuote(quote);
    format.setQuoteEscape(escape);
    format.setComment(comment);
    boolean autoNewline = AUTO_NEWLINE.equalsIgnoreCase(newline);
    if (read) {
      parserSettings = new CsvParserSettings();
      parserSettings.setFormat(format);
      parserSettings.setNullValue(nullValue);
      parserSettings.setEmptyValue(emptyValue);
      // do not use this feature as the parser throws an error if the file
      // has fewer lines than skipRecords;
      // we'll use the skip() operator instead.
      // parserSettings.setNumberOfRowsToSkip(skipRecords);
      parserSettings.setHeaderExtractionEnabled(header);
      parserSettings.setMaxCharsPerColumn(maxCharsPerColumn);
      parserSettings.setMaxColumns(maxColumns);
      parserSettings.setNormalizeLineEndingsWithinQuotes(normalizeLineEndingsInQuotes);
      parserSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces);
      parserSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces);
      parserSettings.setIgnoreLeadingWhitespacesInQuotes(ignoreTrailingWhitespacesInQuotes);
      parserSettings.setIgnoreTrailingWhitespacesInQuotes(ignoreLeadingWhitespacesInQuotes);
      if (autoNewline) {
        parserSettings.setLineSeparatorDetectionEnabled(true);
      } else {
        format.setLineSeparator(newline);
      }
    } else {
      writerSettings = new CsvWriterSettings();
      writerSettings.setFormat(format);
      writerSettings.setNullValue(nullValue);
      writerSettings.setQuoteEscapingEnabled(true);
      writerSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces);
      writerSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces);
      writerSettings.setMaxColumns(maxColumns);
      writerSettings.setNormalizeLineEndingsWithinQuotes(normalizeLineEndingsInQuotes);
      if (autoNewline) {
        format.setLineSeparator(System.lineSeparator());
      } else {
        format.setLineSeparator(newline);
      }
      counter = new AtomicInteger(0);
      writers = new CopyOnWriteArrayList<>();
      if (writeConcurrency() > 1) {
        ThreadFactory threadFactory = new DefaultThreadFactory("csv-connector");
        scheduler =
            maxConcurrentFiles == 1
                ? Schedulers.newSingle(threadFactory)
                : Schedulers.newParallel(maxConcurrentFiles, threadFactory);
      }
      for (int i = 0; i < maxConcurrentFiles; i++) {
        writers.add(new CSVWriter());
      }
    }
  }

  @NotNull
  private List<URL> loadURLs(LoaderConfig settings) {
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

  private void validateURL(LoaderConfig settings, boolean read) {
    if (read) {
      // for LOAD
      if (isPathAbsentOrEmpty(settings, URL)) {
        if (isPathAbsentOrEmpty(settings, URLFILE)) {
          throw new BulkConfigurationException(
              "A URL or URL file is mandatory when using the csv connector for LOAD. Please set connector.csv.url or connector.csv.urlfile "
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
            "A URL is mandatory when using the json connector for UNLOAD. Please set connector.csv.url "
                + "and try again. See settings.md or help for more information.");
      }
    }
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    return (field, cqlType) -> STRING_TYPE_TOKEN;
  }

  @Override
  public boolean supports(ConnectorFeature feature) {
    if (feature instanceof CommonConnectorFeature) {
      CommonConnectorFeature commonFeature = (CommonConnectorFeature) feature;
      switch (commonFeature) {
        case MAPPED_RECORDS:
          // only support mapped records if there is a header defined
          return header;
        case INDEXED_RECORDS:
          // always support indexed records, regardless of the presence of a header
          return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (writers != null) {
      IOException e = null;
      for (CSVWriter writer : writers) {
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

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public Publisher<Record> read() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).flatMap(this::readURL),
        Flux.fromIterable(files).flatMap(this::readURL),
        fluxWithErrorIfAllURLsFailed()
    );
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).map(this::readURL),
        Flux.fromIterable(files).map(this::readURL),
        fluxWithErrorIfAllURLsFailed());
  }

  @NotNull
  private  <T> Flux<T> fluxWithErrorIfAllURLsFailed() {
    return Flux.create(sink -> {
      if(!atLeastOneUrlWasLoadedSuccessfully){
        sink.error(new IOException("None of the provided URLs was loaded successfully."));
        sink.complete();
      } else {
        sink.complete();
      }
    }, FluxSink.OverflowStrategy.ERROR);
  }

  @Override
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    assert !read;
    if (writeConcurrency() > 1) {
      return upstream ->
          Flux.from(upstream)
              .parallel(maxConcurrentFiles)
              .runOn(scheduler)
              .groups()
              .flatMap(rail -> rail.transform(writeRecords(Objects.requireNonNull(rail.key()))));
    } else {
      return upstream -> Flux.from(upstream).transform(writeRecords(0));
    }
  }

  private Function<Flux<Record>, Flux<Record>> writeRecords(int key) {
    CSVWriter writer = writers.get(key);
    return records ->
        records
            .window(flushWindow)
            .flatMap(
                window -> {
                  return window
                      .materialize()
                      .map(
                          signal -> {
                            if (signal.isOnNext()) {
                              Record record = signal.get();
                              assert record != null;
                              try {
                                writer.write(record);
                              } catch (Exception e) {
                                signal = Signal.error(e);
                              }
                            }
                            return signal;
                          })
                      .dematerialize();
                });
  }

  private int writeConcurrency() {
    // when unloading, roots can only be empty or contain one element; if it's empty, we are writing
    // to a single file and the write concurrency is necessarily 1; if it is not empty, we are
    // writing to a directory and the write concurrency is maxConcurrentFiles.
    return roots.isEmpty() ? 1 : maxConcurrentFiles;
  }

  private void tryReadFromDirectories() throws URISyntaxException, IOException {
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
                  "No files in directory {} matched the connector.csv.fileNamePattern of \"{}\".",
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
        files.add(u);
        // not a path on a known filesystem, fall back to reading from URL directly
      }
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
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
              "Invalid value for connector.csv.url: target directory " + root + " must be empty.");
        }
        this.roots.add(root);
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
              long recordNumber = 1;
              LOGGER.debug("Reading {}", url);
              URI resource = URI.create(url.toExternalForm());
              try (Reader r = IOUtils.newBufferedReader(url, encoding)) {
                atLeastOneUrlWasLoadedSuccessfully = true;
                parser.beginParsing(r);
                ParsingContext context = parser.getContext();
                MappedField[] fieldNames = null;
                if (header) {
                  fieldNames = getFieldNames(url, context);
                }
                while (!sink.isCancelled()) {
                  com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
                  String source = context.currentParsedContent();
                  if (row == null) {
                    break;
                  }
                  Record record;
                  try {
                    Object[] values = row.getValues();
                    if (header) {
                      record =
                          DefaultRecord.mapped(
                              source, resource, recordNumber++, fieldNames, values);
                      // also emit indexed fields
                      for (int i = 0; i < values.length; i++) {
                        DefaultIndexedField field = new DefaultIndexedField(i);
                        Object value = values[i];
                        ((DefaultRecord) record).setFieldValue(field, value);
                      }
                    } else {
                      record = DefaultRecord.indexed(source, resource, recordNumber++, values);
                    }
                  } catch (Exception e) {
                    record = new DefaultErrorRecord(source, resource, recordNumber, e);
                  }
                  LOGGER.trace("Emitting record {}", record);
                  controller.awaitRequested(1);
                  sink.next(record);
                }
                LOGGER.debug("Done reading {}", url);
                sink.complete();
              } catch (TextParsingException e) {
                IOException ioe = launderTextParsingException(e, url);
                sink.next(new DefaultErrorRecord(url, resource, recordNumber, ioe));
                sink.complete();
              } catch (Exception e) {
                if (e.getCause() instanceof TextParsingException) {
                  e = launderTextParsingException(((TextParsingException) e.getCause()), url);
                }
                sink.next(new DefaultErrorRecord(url, resource, recordNumber, e));
                sink.complete();
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

  private MappedField[] getFieldNames(URL url, ParsingContext context) throws IOException {
    List<String> fieldNames = new ArrayList<>();
    String[] parsedHeaders = context.parsedHeaders();
    List<String> errors = new ArrayList<>();
    for (int i = 0; i < parsedHeaders.length; i++) {
      String name = parsedHeaders[i];
      // DAT-427: prevent empty names and duplicated names
      if (name == null || name.isEmpty() || WHITESPACE.matcher(name).matches()) {
        errors.add(String.format("found empty field name at index %d", i));
      } else if (fieldNames.contains(name)) {
        errors.add(String.format("found duplicate field name at index %d", i));
      }
      fieldNames.add(name);
    }
    if (errors.isEmpty()) {
      return fieldNames.stream().map(DefaultMappedField::new).toArray(MappedField[]::new);
    } else {
      String msg = url + " has invalid header: " + String.join("; ", errors) + ".";
      throw new IOException(msg);
    }
  }

  private Flux<URL> scanRootDirectory(Path root) {
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

  private class CSVWriter {

    private URL url;
    private CsvWriter writer;

    private void write(Record record) throws IOException {
      if (writer == null) {
        open();
      } else if (shouldRoll()) {
        close();
        open();
      }
      try {
        if (shouldWriteHeader()) {
          writer.writeHeaders(record.fields().stream().map(Field::toString).toArray(String[]::new));
        }
        LOGGER.trace("Writing record {} to {}", record, url);
        writer.writeRow(record.values());
      } catch (TextWritingException e) {
        throw new IOException(String.format("Error writing to %s", url), e);
      }
    }

    private boolean shouldWriteHeader() {
      return header && writer.getRecordCount() == 0;
    }

    private boolean shouldRoll() {
      return !roots.isEmpty() && writer.getRecordCount() == maxRecords;
    }

    private void open() throws IOException {
      url = getOrCreateDestinationURL();
      try {
        writer = new CsvWriter(IOUtils.newBufferedWriter(url, encoding), writerSettings);
        LOGGER.debug("Writing {}", url);
      } catch (RuntimeException | IOException e) {
        throw new IOException(String.format("Error opening %s", url), e);
      }
    }

    private void close() throws IOException {
      if (writer != null) {
        try {
          writer.close();
          LOGGER.debug("Done writing {}", url);
          writer = null;
        } catch (RuntimeException e) {
          // all serious errors are wrapped in an IllegalStateException with no useful information
          throw new IOException(String.format("Error closing %s", url), e.getCause());
        }
      }
    }
  }

  @VisibleForTesting
  URL getOrCreateDestinationURL() {
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

  private IOException launderTextParsingException(TextParsingException e, URL url) {
    // TextParsingException messages are very verbose, so we wrap these exceptions
    // in an IOE that only keeps the first sentence.
    String message = e.getMessage();
    int i = message.indexOf('\n');
    if (i != -1) {
      message = message.substring(0, i);
    }
    if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
      // Extra help for when maxCharsPerColumn is not big enough.
      if (message.matches(
          "Length of parsed input \\(\\d+\\) exceeds the maximum number "
              + "of characters defined in your parser settings.*")) {
        message += "Please increase the value of the connector.csv.maxCharsPerColumn setting.";
      } else {
        // Extra help for when maxColumns is not big enough.
        message +=
            String.format(
                ". The  maximum number of columns per record (%d) was exceeded. "
                    + "Please increase the value of the connector.csv.maxColumns setting.",
                maxColumns);
      }
    }
    return new IOException(
        String.format("Error reading from %s at line %d: %s", url, e.getLineIndex(), message), e);
  }
}
