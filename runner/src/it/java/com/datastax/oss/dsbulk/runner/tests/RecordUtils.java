/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.runner.tests;

import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordUtils {

  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  public static Record mappedCSV(String... tokens) {
    int counter = COUNTER.incrementAndGet();
    DefaultRecord record =
        DefaultRecord.indexed(
            "source" + counter, URI.create("file://file" + counter + ".csv"), counter - 1);
    for (int i = 0; i < tokens.length; i += 2) {
      record.put(new DefaultMappedField(tokens[i]), tokens[i + 1]);
      record.put(new DefaultIndexedField(i % 2), tokens[i + 1]);
    }
    return record;
  }

  public static Record indexedCSV(String... values) {
    int counter = COUNTER.incrementAndGet();
    DefaultRecord record =
        DefaultRecord.indexed(
            "source" + counter, URI.create("file://file" + counter + ".csv"), counter - 1);
    for (int i = 0; i < values.length; i++) {
      record.put(new DefaultIndexedField(i), values[i]);
    }
    return record;
  }

  public static Record mappedJson(String... tokens) {
    int counter = COUNTER.incrementAndGet();
    DefaultRecord record =
        DefaultRecord.indexed(
            "source" + counter, URI.create("file://file" + counter + ".json"), counter - 1);
    for (int i = 0; i < tokens.length; i += 2) {
      record.put(
          new DefaultMappedField(tokens[i]), JsonNodeFactory.instance.textNode(tokens[i + 1]));
    }
    return record;
  }
}
