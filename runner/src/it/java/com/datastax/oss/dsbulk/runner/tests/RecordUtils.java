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

public class RecordUtils {

  public static final URI DEFAULT_RESOURCE = URI.create("file://file1.txt");

  public static Record mappedCSV(URI resource, long position, String... tokens) {
    DefaultRecord record = DefaultRecord.indexed("source" + position, resource, position);
    for (int i = 0; i < tokens.length; i += 2) {
      record.put(new DefaultMappedField(tokens[i]), tokens[i + 1]);
      record.put(new DefaultIndexedField(i / 2), tokens[i + 1]);
    }
    return record;
  }

  public static Record mappedCSV(String... tokens) {
    return mappedCSV(DEFAULT_RESOURCE, 1, tokens);
  }

  public static Record indexedCSV(URI resource, long position, String... values) {
    DefaultRecord record = DefaultRecord.indexed("source" + position, resource, position);
    for (int i = 0; i < values.length; i++) {
      record.put(new DefaultIndexedField(i), values[i]);
    }
    return record;
  }

  public static Record indexedCSV(String... values) {
    return indexedCSV(DEFAULT_RESOURCE, 1, values);
  }

  public static Record mappedJson(URI resource, long position, String... tokens) {
    DefaultRecord record = DefaultRecord.indexed("source" + position, resource, position);
    for (int i = 0; i < tokens.length; i += 2) {
      record.put(
          new DefaultMappedField(tokens[i]), JsonNodeFactory.instance.textNode(tokens[i + 1]));
    }
    return record;
  }

  public static Record mappedJson(String... tokens) {
    return mappedJson(DEFAULT_RESOURCE, 1, tokens);
  }

  public static Record error(URI resource, long position, Throwable error) {
    return new DefaultErrorRecord("source" + position, resource, position, error);
  }

  public static Record error(Throwable error) {
    return error(DEFAULT_RESOURCE, 1, error);
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
