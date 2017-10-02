/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv.internal;

import static com.datastax.dsbulk.connectors.csv.CSVConnector.openOutputStream;

import com.datastax.dsbulk.connectors.api.Record;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;

public class CSVRecordSubscriber extends BaseSubscriber<Record> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecordSubscriber.class);

  private final CsvWriterSettings writerSettings;
  private final Path root;
  private final String fileNameFormat;
  private final AtomicInteger counter;
  private final boolean header;
  private final long maxLines;

  private URL url;
  private CsvWriter writer;

  public CSVRecordSubscriber(
      CsvWriterSettings writerSettings,
      URL url,
      Path root,
      String fileNameFormat,
      AtomicInteger counter,
      boolean header,
      long maxLines) {
    this.writerSettings = writerSettings;
    this.url = url;
    this.root = root;
    this.fileNameFormat = fileNameFormat;
    this.counter = counter;
    this.header = header;
    this.maxLines = maxLines;
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

  private void start() {
    url = getOrCreateDestinationURL();
    writer = createCSVWriter(url);
    LOGGER.debug("Writing " + url);
  }

  private void end() {
    LOGGER.debug("Done writing {}", url);
    if (writer != null) {
      writer.close();
    }
  }

  private CsvWriter createCSVWriter(URL url) {
    try {
      return new CsvWriter(openOutputStream(url), writerSettings);
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
}
