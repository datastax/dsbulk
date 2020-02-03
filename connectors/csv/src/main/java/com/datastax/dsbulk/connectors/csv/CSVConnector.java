/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.csv;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isValueFromReferenceConfig;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.io.CompressedIOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.connectors.api.AbstractConnector;
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.dsbulk.connectors.api.ConnectorWriter;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.MappedField;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultIndexedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.TextWritingException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
public class CSVConnector extends AbstractConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnector.class);
  private static final GenericType<String> STRING_TYPE = GenericType.STRING;
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private static final String ENCODING = "encoding";
  private static final String COMPRESSION = "compression";
  private static final String DELIMITER = "delimiter";
  private static final String QUOTE = "quote";
  private static final String ESCAPE = "escape";
  private static final String COMMENT = "comment";
  private static final String NEWLINE = "newline";
  private static final String HEADER = "header";
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

  private List<URL> urls;
  private String pattern;
  private Charset encoding;
  private String compression;
  private char delimiter;
  private char quote;
  private char escape;
  private char comment;
  private String newline;
  private long skipRecords;
  private long maxRecords;
  private int maxConcurrentFiles;
  private boolean header;
  private int maxCharsPerColumn;
  private int maxColumns;
  private boolean ignoreLeadingWhitespaces;
  private boolean ignoreTrailingWhitespaces;
  private boolean ignoreTrailingWhitespacesInQuotes;
  private boolean ignoreLeadingWhitespacesInQuotes;
  private boolean normalizeLineEndingsInQuotes;
  private String nullValue;
  private String emptyValue;
  private CsvParserSettings parserSettings;
  private CsvWriterSettings writerSettings;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      validateURL(settings, read);
      this.read = read;
      urls = loadURLs(settings);
      roots = new ArrayList<>();
      files = new ArrayList<>();
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
      if (!CompressedIOUtils.isNoneCompression(compression)
          && isValueFromReferenceConfig(settings, FILE_NAME_FORMAT)) {
        fileNameFormat = fileNameFormat + CompressedIOUtils.getCompressionSuffix(compression);
      }
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
      if (!AUTO_NEWLINE.equalsIgnoreCase(newline) && (newline.isEmpty() || newline.length() > 2)) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for connector.csv.%s: Expecting '%s' or a string containing 1 or 2 chars, got: '%s'",
                NEWLINE, AUTO_NEWLINE, newline));
      }
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(e, "dsbulk.connector.csv");
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
      // DAT-516: Always quote comment character when unloading
      writerSettings.setQuotationTriggers(comment);
      counter = new AtomicInteger(0);
    }
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    return (field, cqlType) -> STRING_TYPE;
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
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    return super.write(new CSVWriter(), "csv-connector");
  }

  public Flux<Record> readURL(URL url) {
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
              try (Reader r = CompressedIOUtils.newBufferedReader(url, encoding, compression)) {
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
                sink.error(ioe);
              } catch (Exception e) {
                if (e.getCause() instanceof TextParsingException) {
                  e = launderTextParsingException(((TextParsingException) e.getCause()), url);
                }
                sink.error(
                    new IOException(
                        String.format("Error reading from %s at line %d", url, recordNumber), e));
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

  private class CSVWriter implements ConnectorWriter {

    private URL url;
    private CsvWriter writer;

    public void write(Record record) throws IOException {
      try {
        if (writer == null) {
          open();
        } else if (shouldRoll()) {
          close();
          open();
        }
        if (shouldWriteHeader()) {
          writer.writeHeaders(record.fields().stream().map(Field::toString).toArray(String[]::new));
        }
        LOGGER.trace("Writing record {} to {}", record, url);
        writer.writeRow(record.values());
      } catch (TextWritingException e) {
        if (!(e.getCause() instanceof ClosedChannelException)) {
          throw new IOException(String.format("Error writing to %s", url), e);
        }
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
        writer =
            new CsvWriter(
                CompressedIOUtils.newBufferedWriter(url, encoding, compression), writerSettings);
        LOGGER.debug("Writing {}", url);
      } catch (ClosedChannelException e) {
        // OK, happens when the channel was closed due to interruption
      } catch (RuntimeException | IOException e) {
        throw new IOException(String.format("Error opening %s", url), e);
      }
    }

    public void close() throws IOException {
      if (writer != null) {
        try {
          writer.close();
          LOGGER.debug("Done writing {}", url);
          writer = null;
        } catch (RuntimeException e) {
          // all serious errors are wrapped in an IllegalStateException with no useful information
          if (!(e.getCause() instanceof ClosedChannelException)) {
            throw new IOException(String.format("Error closing %s", url), e.getCause());
          }
        }
      }
    }
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
