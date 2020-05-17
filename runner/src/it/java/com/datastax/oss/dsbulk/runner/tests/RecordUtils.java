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
package com.datastax.oss.dsbulk.runner.tests;

import com.datastax.oss.dsbulk.connectors.api.DefaultErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
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

  public static Record error(Throwable error) {
    int counter = COUNTER.incrementAndGet();
    return new DefaultErrorRecord(
        "source" + counter, URI.create("file://file" + counter + ".csv"), counter - 1, error);
  }

  public static Record cloneRecord(Record record) {
    if (record instanceof ErrorRecord) {
      return new DefaultErrorRecord(
          record.getSource(),
          record.getResource(),
          record.getPosition(),
          ((ErrorRecord) record).getError());
    } else {
      DefaultRecord clone =
          new DefaultRecord(record.getSource(), record.getResource(), record.getPosition());
      for (Field field : record.fields()) {
        clone.setFieldValue(field, record.getFieldValue(field));
      }
      return clone;
    }
  }
}
