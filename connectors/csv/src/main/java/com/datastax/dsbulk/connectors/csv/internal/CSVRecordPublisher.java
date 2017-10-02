/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv.internal;

import static com.datastax.dsbulk.connectors.csv.CSVConnector.getCurrentLocation;
import static com.datastax.dsbulk.connectors.csv.CSVConnector.openInputStream;

import com.datastax.dsbulk.commons.internal.reactive.AbstractEventBasedPublisher;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.google.common.base.Suppliers;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvParser;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVRecordPublisher extends AbstractEventBasedPublisher<Record> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecordPublisher.class);

  private final CsvParser parser;
  private final URL url;
  private final Charset encoding;
  private final boolean header;

  private InputStream is;

  public CSVRecordPublisher(CsvParser parser, URL url, Charset encoding, boolean header) {
    this.parser = parser;
    this.url = url;
    this.encoding = encoding;
    this.header = header;
  }

  @Override
  protected void checkOnDataAvailable() {
    onDataAvailable();
  }

  @Nullable
  @Override
  protected Record read() throws IOException {
    if (is == null) {
      LOGGER.debug("Reading {}", url);
      is = openInputStream(url);
      parser.beginParsing(is, encoding);
    }
    com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
    ParsingContext context = parser.getContext();
    String source = context.currentParsedContent();
    if (row == null) {
      parser.stopParsing();
      is.close();
      onAllDataRead();
      LOGGER.debug("Done reading {}", url);
      return null;
    }
    Record record;
    if (header) {
      record =
          new DefaultRecord(
              source,
              Suppliers.memoize(() -> getCurrentLocation(url, context)),
              context.parsedHeaders(),
              (Object[]) row.getValues());
    } else {
      record =
          new DefaultRecord(
              source,
              Suppliers.memoize(() -> getCurrentLocation(url, context)),
              (Object[]) row.getValues());
    }
    LOGGER.trace("Emitting record {}", record);
    return record;
  }
}
