/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TTL_VARNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecordMetadata;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class DefaultRecordMapperTest {

  private static final String C1 = "col1";
  private static final String C2 = "col2";
  private static final String C3 = "My Fancy Column Name";

  private Mapping mapping;
  private Record record;
  private PreparedStatement insertStatement;
  private BoundStatement boundStatement;
  private ArgumentCaptor<String> variableCaptor;
  private ArgumentCaptor<Object> valueCaptor;
  private ArgumentCaptor<TypeCodec> codecCaptor;
  private RecordMetadata recordMetadata;
  private final URI location = URI.create("file://file1?line=1");
  private final DateTimeFormatter timestampFormat = CodecSettings.CQL_DATE_TIME_FORMAT;

  @Before
  public void setUp() throws Exception {
    variableCaptor = ArgumentCaptor.forClass(String.class);
    valueCaptor = ArgumentCaptor.forClass(Object.class);
    codecCaptor = ArgumentCaptor.forClass(TypeCodec.class);

    recordMetadata =
        new DefaultRecordMetadata(
            ImmutableMap.of(
                "0",
                TypeToken.of(Integer.class),
                "1",
                TypeToken.of(String.class),
                "2",
                TypeToken.of(String.class)));

    boundStatement = mock(BoundStatement.class);
    mapping = mock(Mapping.class);
    record = mock(Record.class);
    insertStatement = mock(PreparedStatement.class);
    ColumnDefinitions variables = mock(ColumnDefinitions.class);

    when(boundStatement.preparedStatement()).thenReturn(insertStatement);
    when(insertStatement.getVariables()).thenReturn(variables);
    when(variables.getType(C1)).thenReturn(DataType.cint());
    when(variables.getType(C2)).thenReturn(DataType.varchar());
    when(variables.getType(C3)).thenReturn(DataType.varchar());
    when(variables.getType(TTL_VARNAME)).thenReturn(DataType.cint());
    when(variables.getType(TIMESTAMP_VARNAME)).thenReturn(DataType.bigint());

    when(record.fields()).thenReturn(Sets.newHashSet("0", "1", "2"));
    when(record.getFieldValue("0")).thenReturn(42);
    when(record.getFieldValue("2")).thenReturn("NULL");
    when(record.getSource()).thenReturn("source");
    when(record.getLocation()).thenReturn(location);

    when(mapping.fieldToVariable("0")).thenReturn(C1);
    when(mapping.fieldToVariable("1")).thenReturn(C2);
    when(mapping.fieldToVariable("2")).thenReturn(C3);

    TypeCodec codec1 = TypeCodec.cint();
    TypeCodec codec2 = TypeCodec.varchar();
    TypeCodec codec3 = TypeCodec.bigint();

    when(mapping.codec(C1, DataType.cint(), TypeToken.of(Integer.class))).thenReturn(codec1);
    when(mapping.codec(C2, DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    when(mapping.codec(TTL_VARNAME, DataType.cint(), TypeToken.of(String.class)))
        .thenReturn(codec1);
    when(mapping.codec(TTL_VARNAME, DataType.cint(), TypeToken.of(Integer.class)))
        .thenReturn(codec1);
    when(mapping.codec(TIMESTAMP_VARNAME, DataType.bigint(), TypeToken.of(String.class)))
        .thenReturn(codec3);
    when(mapping.codec(TIMESTAMP_VARNAME, DataType.bigint(), TypeToken.of(Long.class)))
        .thenReturn(codec3);
  }

  @Test
  public void should_map_string_int_columns() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, C3, "NULL", TypeCodec.varchar());
  }

  @Test
  public void should_bind_static_ttl() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            30,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(4))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, C3, "NULL", TypeCodec.varchar());
    assertParameter(3, "dsbulk_internal_ttl", 30, TypeCodec.cint());
  }

  @Test
  public void should_bind_static_timestamp() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            123987,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(4))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, C3, "NULL", TypeCodec.varchar());
    assertParameter(3, "dsbulk_internal_timestamp", 123987L, TypeCodec.bigint());
  }

  @Test
  public void should_bind_mapped_ttl() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("123");
    when(mapping.fieldToVariable("2")).thenReturn(TTL_VARNAME);
    when(mapping.variableToField(TTL_VARNAME)).thenReturn("2");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, "dsbulk_internal_ttl", "123", TypeCodec.cint());
  }

  @Test
  public void should_bind_mapped_long_timestamp() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("123");
    when(mapping.fieldToVariable("2")).thenReturn(TIMESTAMP_VARNAME);
    when(mapping.variableToField(TIMESTAMP_VARNAME)).thenReturn("2");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, "dsbulk_internal_timestamp", 123L, TypeCodec.bigint());
  }

  @Test
  public void should_bind_mapped_string_timestamp() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("2017-01-02T00:00:02");
    when(mapping.fieldToVariable("2")).thenReturn(TIMESTAMP_VARNAME);
    when(mapping.variableToField(TIMESTAMP_VARNAME)).thenReturn("2");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, "dsbulk_internal_timestamp", 1483315202000000L, TypeCodec.bigint());
  }

  @Test
  public void should_bind_mapped_custom_string_timestamp() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("20171123123456");
    when(mapping.fieldToVariable("2")).thenReturn(TIMESTAMP_VARNAME);
    when(mapping.variableToField(TIMESTAMP_VARNAME)).thenReturn("20171123123456");
    DateTimeFormatter timestampFormat =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);
    ZonedDateTime zonedDateTime = ZonedDateTime.parse("20171123123456", timestampFormat);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(
        2,
        "dsbulk_internal_timestamp",
        TimeUnit.SECONDS.toMicros(zonedDateTime.toEpochSecond()),
        TypeCodec.bigint());
  }

  @Test
  public void should_bind_mapped_ttl_over_static() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("123");
    when(mapping.fieldToVariable("2")).thenReturn(TTL_VARNAME);
    when(mapping.variableToField(TTL_VARNAME)).thenReturn("2");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            30,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, "dsbulk_internal_ttl", "123", TypeCodec.cint());
  }

  @Test
  public void should_bind_mapped_timestamp_over_static() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    when(record.getFieldValue("2")).thenReturn("123");
    when(mapping.fieldToVariable("2")).thenReturn(TIMESTAMP_VARNAME);
    when(mapping.variableToField(TIMESTAMP_VARNAME)).thenReturn("2");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            30,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C2, "NIL", TypeCodec.varchar());
    assertParameter(2, "dsbulk_internal_timestamp", 123L, TypeCodec.bigint());
  }

  @Test
  public void should_map_null_to_unset() throws Exception {
    when(record.getFieldValue("1")).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C3, "NULL", TypeCodec.varchar());
  }

  @Test
  public void should_map_null_words_to_unset() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of("NIL", "NULL"),
            true,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement)
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
  }

  @Test
  public void should_map_null_to_null() throws Exception {
    when(record.getFieldValue("1")).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            false,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C3, "NULL", TypeCodec.varchar());

    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo(C2);
  }

  @Test
  public void should_map_null_word_to_null() throws Exception {
    when(record.getFieldValue("1")).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of("NIL"),
            false,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());
    assertParameter(1, C3, "NULL", TypeCodec.varchar());

    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo(C2);
  }

  @Test
  public void should_return_unmappable_statement_when_mapping_fails() throws Exception {
    //noinspection unchecked
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class)))
        .thenThrow(CodecNotFoundException.class);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            false,
            -1,
            -1,
            timestampFormat,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    assertThat(((UnmappableStatement) result).getSource()).isEqualTo(record);
    assertThat(((UnmappableStatement) result).getLocation().toString())
        .isEqualTo(location.toString() + "&field=2&My+Fancy+Column+Name=NULL&cqlType=varchar");

    //noinspection unchecked
    verify(boundStatement, times(1))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, C1, 42, TypeCodec.cint());

    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo(C2);
  }

  private void assertParameter(
      int index, String expectedVariable, Object expectedValue, TypeCodec<?> expectedCodec) {
    assertThat(variableCaptor.getAllValues().get(index))
        .isEqualTo(Metadata.quoteIfNecessary(expectedVariable));
    assertThat(valueCaptor.getAllValues().get(index)).isEqualTo(expectedValue);
    assertThat(codecCaptor.getAllValues().get(index)).isSameAs(expectedCodec);
  }
}
