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
package com.datastax.oss.dsbulk.format.row;

import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.dsbulk.tests.DataTypesProvider;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.simulacron.common.codec.InvalidTypeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

class RowFormatterTest {

  private final ProtocolVersion version = ProtocolVersion.DEFAULT;

  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

  // Basic Tests

  @Test
  void should_format_simple_row_with_named_values() {
    RowFormatter formatter = new RowFormatter();
    Row row = mockRow("foo", 42, true, null);
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s)
        .contains("4 values")
        .contains("c1: 'foo'")
        .contains("c2: 42")
        .contains("c3: true")
        .contains("c4: <NULL>");
  }

  @Test
  void should_use_toString_when_exception_during_formatting() {
    RowFormatter formatter = new RowFormatter();
    Row row = mock(Row.class);
    when(row.getColumnDefinitions()).thenThrow(new NullPointerException());
    assertThat(formatter.format(row, version, codecRegistry)).isEqualTo(row.toString());
  }

  @Test
  void should_use_generic_description_exception_during_formatting_and_toString() {
    RowFormatter formatter = new RowFormatter();
    Row row = mock(Row.class);
    when(row.getColumnDefinitions()).thenThrow(new NullPointerException());
    when(row.toString()).thenThrow(new NullPointerException());
    assertThat(formatter.format(row, version, codecRegistry)).isEqualTo("row[?]");
  }

  // Limits

  @Test
  void should_not_print_more_bound_values_than_max() {
    RowFormatter formatter = new RowFormatter(-1, 2);
    Row row = mockRow("foo", 42, true);
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s)
        .contains("3 values")
        .contains("c1: 'foo'")
        .contains("c2: 42")
        .contains("...")
        .doesNotContain("c3: true");
  }

  @Test
  void should_truncate_bound_value() {
    RowFormatter formatter = new RowFormatter(2, -1);
    Row row = mockRow("foo", 42, true);
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s)
        .contains("3 values")
        .contains("c1: 'f...")
        .contains("c2: 42")
        .contains("c3: tr...");
  }

  @Test
  void should_truncate_bound_value_byte_buffer() {
    RowFormatter formatter = new RowFormatter(4, -1);
    Row row = mockRow(Bytes.fromHexString("0xCAFEBABE"));
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s).contains("c1: 0xca...");
  }

  @Test
  void should_format_malformed_byte_buffer() {
    RowFormatter formatter = new RowFormatter();
    Row row = mockRow(42);
    when(row.getObject(0))
        .thenThrow(
            new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got 5"));
    when(row.getBytesUnsafe(0)).thenReturn(Bytes.fromHexString("0x0102030405"));
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s).contains("c1: 0x0102030405 (malformed buffer for type INT)");
  }

  // Data types

  @ParameterizedTest
  @ArgumentsSource(DataTypesProvider.class)
  void should_log_all_parameter_types(DataType type, Object value) {
    RowFormatter formatter = new RowFormatter(-1, -1);
    Row row = mockRow(value);
    String s = formatter.format(row, version, codecRegistry);
    // time cannot be used with simple rows
    TypeCodec<Object> codec = codecRegistry.codecFor(type, value);
    assertThat(s).contains(codec.format(value));
  }
}
