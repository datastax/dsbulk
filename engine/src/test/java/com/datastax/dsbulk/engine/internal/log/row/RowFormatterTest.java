/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.row;

import static com.datastax.driver.core.DataType.blob;
import static com.datastax.driver.core.DataType.cboolean;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.log.DataTypesProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

class RowFormatterTest {

  private final ProtocolVersion version = ProtocolVersion.NEWEST_SUPPORTED;

  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;

  // Basic Tests

  @Test
  void should_format_simple_row_with_named_values() {
    RowFormatter formatter = new RowFormatter();
    Row row = mockRow();
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s)
        .contains("3 values")
        .contains("c1: 'foo'")
        .contains("c2: 42")
        .contains("c3: true");
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
    Row row = mockRow();
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
    Row row = mockRow();
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
    Row row = mock(Row.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(cd);
    when(cd.size()).thenReturn(1);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getType(0)).thenReturn(blob());
    when(row.getObject(0)).thenReturn(Bytes.fromHexString("0xCAFEBABE"));
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s).contains("c1: 0xca...");
  }

  @Test
  void should_format_malformed_byte_buffer() {
    RowFormatter formatter = new RowFormatter();
    Row row = mock(Row.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(cd);
    when(cd.size()).thenReturn(1);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getType(0)).thenReturn(cint());
    when(row.getObject(0))
        .thenThrow(
            new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got 5"));
    when(row.getBytesUnsafe(0)).thenReturn(Bytes.fromHexString("0x0102030405"));
    String s = formatter.format(row, version, codecRegistry);
    assertThat(s).contains("c1: 0x0102030405 (malformed buffer for type int)");
  }

  // Data types

  @ParameterizedTest
  @ArgumentsSource(DataTypesProvider.class)
  void should_log_all_parameter_types(DataType type, Object value) {
    RowFormatter formatter = new RowFormatter(-1, -1);
    Row row = mock(Row.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(cd);
    when(cd.size()).thenReturn(1);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getType(0)).thenReturn(type);
    when(row.getObject(0)).thenReturn(value);
    String s = formatter.format(row, version, codecRegistry);
    // time cannot be used with simple rows
    TypeCodec<Object> codec = codecRegistry.codecFor(type, value);
    assertThat(s).contains(codec.format(value));
  }

  @NotNull
  private Row mockRow() {
    Row row = mock(Row.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(cd);
    when(cd.size()).thenReturn(3);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getName(1)).thenReturn("c2");
    when(cd.getName(2)).thenReturn("c3");
    when(cd.getType(0)).thenReturn(varchar());
    when(cd.getType(1)).thenReturn(cint());
    when(cd.getType(2)).thenReturn(cboolean());
    when(row.getObject(0)).thenReturn("foo");
    when(row.getObject(1)).thenReturn(42);
    when(row.getObject(2)).thenReturn(true);
    return row;
  }
}
