/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import com.datastax.loader.connectors.api.internal.ArrayBackedRecord;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import org.reactivestreams.Publisher;

/** */
public class CSVConnector implements Connector {

  // TODO can be a file or a directory, for the time being, it is assumed it is a file
  private URL url;

  @Override
  public Publisher<Record> read() {
    // TODO replace with a full-blown Publisher<Record> to improve backpressure, see ResultPublisher
    return Flowable.create(
        e -> {
          CsvParserSettings settings = new CsvParserSettings();
          settings.setHeaderExtractionEnabled(true);
          CsvParser parser = new CsvParser(settings);
          try (InputStream is = new BufferedInputStream(url.openStream())) {
            parser.beginParsing(is, "UTF-8");
            com.univocity.parsers.common.record.Record row;
            while ((row = parser.parseNextRecord()) != null) {
              if (e.isCancelled()) break;
              e.onNext(new ArrayBackedRecord(row.getValues()));
            }
            e.onComplete();
            parser.stopParsing();
          }
        },
        BackpressureStrategy.BUFFER);
  }

  @Override
  public void configure(Map<String, Object> settings) {
    // TODO: encoding, delim, escape char, file or directory, header, footer, comment lines, null word...
  }
}
