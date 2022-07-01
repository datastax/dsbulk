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
package com.datastax.oss.dsbulk.connectors.csv;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.DefaultErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.MappedField;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.connectors.commons.AbstractFileBasedConnector;
import com.datastax.oss.dsbulk.io.CompressedIOUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.TextWritingException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SynchronousSink;

/**
 * A connector for CSV files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link URLStreamHandler
 * handler} installed for it. For file URLs, it is also capable of reading several files at once
 * from a given root directory.
 *
 * <p>This connector is highly configurable; see its {@code dsbulk-reference.conf} file, bundled
 * within its jar archive, for detailed information.
 */
public class CSVConnector extends AbstractFileBasedConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnector.class);
  private static final GenericType<String> STRING_TYPE = GenericType.STRING;
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

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
  private static final String AUTO = "AUTO";

  private String delimiter;
  private char quote;
  private char escape;
  private char comment;
  private String newline;
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
  @NonNull
  public String getConnectorName() {
    return "csv";
  }

  @Override
  public void configure(@NonNull Config settings, boolean read, boolean retainRecordSources) {
    try {
      super.configure(settings, read, retainRecordSources);
      delimiter = settings.getString(DELIMITER);
      if (delimiter.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for dsbulk.connector.csv.%s: Expecting non-empty string",
                DELIMITER));
      }
      quote = ConfigUtils.getChar(settings, QUOTE);
      escape = ConfigUtils.getChar(settings, ESCAPE);
      comment = ConfigUtils.getChar(settings, COMMENT);
      header = settings.getBoolean(HEADER);
      maxCharsPerColumn = settings.getInt(MAX_CHARS_PER_COLUMN);
      maxColumns = settings.getInt(MAX_COLUMNS);
      newline = settings.getString(NEWLINE);
      ignoreLeadingWhitespaces = settings.getBoolean(IGNORE_LEADING_WHITESPACES);
      ignoreTrailingWhitespaces = settings.getBoolean(IGNORE_TRAILING_WHITESPACES);
      ignoreLeadingWhitespacesInQuotes = settings.getBoolean(IGNORE_LEADING_WHITESPACES_IN_QUOTES);
      ignoreTrailingWhitespacesInQuotes =
          settings.getBoolean(IGNORE_TRAILING_WHITESPACES_IN_QUOTES);
      normalizeLineEndingsInQuotes = settings.getBoolean(NORMALIZE_LINE_ENDINGS_IN_QUOTES);
      nullValue = settings.getIsNull(NULL_VALUE) ? null : settings.getString(NULL_VALUE);
      emptyValue = settings.getIsNull(EMPTY_VALUE) ? null : settings.getString(EMPTY_VALUE);
      if (!AUTO_NEWLINE.equalsIgnoreCase(newline) && (newline.isEmpty() || newline.length() > 2)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for dsbulk.connector.csv.%s: Expecting '%s' or a string containing 1 or 2 chars, got: '%s'",
                NEWLINE, AUTO_NEWLINE, newline));
      }
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.connector.csv");
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    super.init();
    CsvFormat format = new CsvFormat();
    format.setDelimiter(delimiter);
    format.setQuote(quote);
    format.setQuoteEscape(escape);
    format.setComment(comment);
    boolean autoNewline = AUTO_NEWLINE.equalsIgnoreCase(newline);
    if (read) {
      parserSettings = new CsvParserSettings();
      parserSettings.setFormat(format);
      parserSettings.setNullValue(AUTO.equalsIgnoreCase(nullValue) ? null : nullValue);
      parserSettings.setEmptyValue(AUTO.equalsIgnoreCase(emptyValue) ? "" : emptyValue);
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
      parserSettings.setIgnoreLeadingWhitespacesInQuotes(ignoreLeadingWhitespacesInQuotes);
      parserSettings.setIgnoreTrailingWhitespacesInQuotes(ignoreTrailingWhitespacesInQuotes);
      if (autoNewline) {
        parserSettings.setLineSeparatorDetectionEnabled(true);
      } else {
        format.setLineSeparator(newline);
      }
    } else {
      writerSettings = new CsvWriterSettings();
      writerSettings.setFormat(format);
      writerSettings.setNullValue(AUTO.equalsIgnoreCase(nullValue) ? null : nullValue);
      // DAT-605: use empty quoted fields by default to distinguish empty strings from nulls
      writerSettings.setEmptyValue(
          AUTO.equalsIgnoreCase(emptyValue) ? "" + quote + quote : emptyValue);
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
    }
  }

  @NonNull
  @Override
  public RecordMetadata getRecordMetadata() {
    return (field, cqlType) -> STRING_TYPE;
  }

  @Override
  public boolean supports(@NonNull ConnectorFeature feature) {
    if (feature instanceof CommonConnectorFeature) {
      CommonConnectorFeature commonFeature = (CommonConnectorFeature) feature;
      switch (commonFeature) {
        case MAPPED_RECORDS:
          // only support mapped records if there is a header defined
          return header;
        case INDEXED_RECORDS:
          // always support indexed records, regardless of the presence of a header
          return true;
        case DATA_SIZE_SAMPLING:
          return isDataSizeSamplingAvailable();
      }
    }
    return false;
  }

  @Override
  @NonNull
  protected RecordReader newSingleFileReader(@NonNull URL url, URI resource) throws IOException {
    return new CSVRecordReader(url, resource);
  }

  private class CSVRecordReader implements RecordReader {

    private final URL url;
    private final URI resource;
    private final CsvParser parser;
    private final ParsingContext context;
    private final MappedField[] fieldNames;

    private long recordNumber = 1;

    private CSVRecordReader(URL url, URI resource) throws IOException {
      this.url = url;
      this.resource = resource;
      try {
        parser = new CsvParser(parserSettings);
        Reader r = CompressedIOUtils.newBufferedReader(url, encoding, compression);
        parser.beginParsing(r);
        context = parser.getContext();
        fieldNames = header ? getFieldNames(url, context) : null;
      } catch (Exception e) {
        throw asIOException(url, e, "Error creating CSV parser for " + url);
      }
    }

    private MappedField[] getFieldNames(URL url, ParsingContext context) throws IOException {
      List<String> fieldNames = new ArrayList<>();
      String[] parsedHeaders = context.headers();
      if (parsedHeaders == null) {
        throw new IOException(
            String.format("The parsed headers from provided url: %s are null", url));
      }
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

    @NonNull
    @Override
    public RecordReader readNext(@NonNull SynchronousSink<Record> sink) {
      try {
        com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
        if (row != null) {
          Record record = parseNext(row);
          LOGGER.trace("Emitting record {}", record);
          sink.next(record);
        } else {
          LOGGER.debug("Done reading {}", url);
          sink.complete();
        }
      } catch (Exception e) {
        IOException error =
            asIOException(
                url, e, String.format("Error reading from %s at line %d", url, recordNumber));
        sink.error(error);
      }
      return this;
    }

    @NonNull
    private Record parseNext(com.univocity.parsers.common.record.Record row) {
      String source = retainRecordSources ? context.currentParsedContent() : null;
      Record record;
      try {
        Object[] values = row.getValues();
        if (header) {
          record = DefaultRecord.mapped(source, resource, recordNumber++, fieldNames, values);
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
      return record;
    }

    @Override
    public void close() {
      if (parser != null) {
        parser.stopParsing();
      }
    }
  }

  @NonNull
  @Override
  protected RecordWriter newSingleFileWriter() {
    return new CSVRecordWriter();
  }

  private class CSVRecordWriter implements RecordWriter {

    private URL url;
    private CsvWriter writer;

    @Override
    public void write(@NonNull Record record) throws IOException {
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

    @Override
    public void flush() throws IOException {
      if (writer != null) {
        try {
          writer.flush();
        } catch (RuntimeException e) {
          throw new IOException(String.format("Error flushing %s", url), e);
        }
      }
    }

    @Override
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

  @NonNull
  private IOException asIOException(@NonNull URL url, Exception e, String genericErrorMessage) {
    IOException error;
    if (e instanceof TextParsingException) {
      error = launderTextParsingException(((TextParsingException) e), url);
    } else if (e.getCause() instanceof TextParsingException) {
      error = launderTextParsingException(((TextParsingException) e.getCause()), url);
    } else {
      error = new IOException(genericErrorMessage, e);
    }
    return error;
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
