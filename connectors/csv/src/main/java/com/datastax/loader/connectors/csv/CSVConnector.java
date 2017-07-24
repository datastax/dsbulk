/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.internal.ArrayRecord;
import com.datastax.loader.connectors.api.internal.ErrorRecord;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.RowProcessorErrorHandler;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;
import org.reactivestreams.Publisher;

/** */
public class CSVConnector implements Connector {

  // TODO can be a file or a directory or stdin, for the time being, it is assumed it is a file
  private URL url;
  private Charset encoding;

  @Override
  public Publisher<Record> read() {
    // TODO if reading from several files, use Flowable.merge(flowable1, flowable2,...); each flowable should be subscribed to in a scheduler e.g. Schedulers.io()
    // TODO replace with a full-blown Publisher<Record> to improve backpressure, see ResultPublisher
    return Flowable.create(
        e -> {
          CsvParserSettings settings = new CsvParserSettings();
          settings.setHeaderExtractionEnabled(true);
          settings.setProcessorErrorHandler(
              (RowProcessorErrorHandler)
                  (error, inputRow, context) -> {
                    // this is actually never going to be called;
                    // DataProcessingExceptions are only thrown by processors and we don't use any.
                    // Actually, the parser is almost infallible. And when it fails,
                    // the error is unrecoverable and this method is not called.
                    String source = context.currentParsedContent();
                    URL location = getLocation(context);
                    if (!e.isCancelled()) e.onNext(new ErrorRecord(source, location, error));
                  });
          CsvParser parser = new CsvParser(settings);
          try (InputStream is = new BufferedInputStream(url.openStream())) {
            parser.beginParsing(is, encoding);
            while (true) {
              com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
              ParsingContext context = parser.getContext();
              String source = context.currentParsedContent();
              URL location = getLocation(context);
              if (row == null) break;
              if (e.isCancelled()) break;
              e.onNext(new ArrayRecord(source, location, row.getValues()));
            }
            e.onComplete();
            parser.stopParsing();
          }
        },
        BackpressureStrategy.BUFFER);
  }

  private URL getLocation(ParsingContext context) {
    URL location;
    try {
      long line = context.currentLine();
      int column = context.currentColumn();
      location = new URL(url.toExternalForm() + "?line=" + line + "&column=" + column);
    } catch (MalformedURLException ignored) {
      location = url;
    }
    return location;
  }

  @Override
  public void configure(Map<String, Object> settings) throws MalformedURLException {
    url = new URL((String) settings.get("url"));
    encoding = Charset.forName((String) settings.getOrDefault("encoding", "UTF-8"));
    // TODO: encoding, delim, escape char, file or directory, header, footer, comment lines, null word...
  }
}
