/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TTL_VARNAME;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
import com.datastax.dsbulk.engine.internal.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class DefaultRecordMapperTest {

  private static final String F1 = "field1";
  private static final String F2 = "field2";
  private static final String F3 = "field3";

  private static final String C1 = "col1";
  private static final String C2 = "col2";
  private static final String C3 = "My Fancy Column Name";

  private final URI location = URI.create("file://file1?line=1");
  private final TypeCodec codec1 = new StringToIntegerCodec(null, null, null, null);
  private final TypeCodec codec2 = new StringToLongCodec(null, null, null, null);
  private final TypeCodec codec3 = TypeCodec.varchar();

  private Mapping mapping;
  private Record record;
  private PreparedStatement insertStatement;
  private BoundStatement boundStatement;
  private ArgumentCaptor<String> variableCaptor;
  private ArgumentCaptor<Object> valueCaptor;
  private ArgumentCaptor<TypeCodec> codecCaptor;
  private RecordMetadata recordMetadata;
  private ThreadLocal<DecimalFormat> formatter =
      ThreadLocal.withInitial(
          () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US)));

  @BeforeEach
  void setUp() throws Exception {
    variableCaptor = ArgumentCaptor.forClass(String.class);
    valueCaptor = ArgumentCaptor.forClass(Object.class);
    codecCaptor = ArgumentCaptor.forClass(TypeCodec.class);

    recordMetadata =
        new DefaultRecordMetadata(
            ImmutableMap.of(
                F1,
                TypeToken.of(String.class),
                F2,
                TypeToken.of(String.class),
                F3,
                TypeToken.of(String.class)));

    boundStatement = mock(BoundStatement.class);
    mapping = mock(Mapping.class);
    record = mock(Record.class);
    insertStatement = mock(PreparedStatement.class);
    ColumnDefinitions variables = mock(ColumnDefinitions.class);

    when(boundStatement.preparedStatement()).thenReturn(insertStatement);
    when(insertStatement.getVariables()).thenReturn(variables);

    when(variables.getType(C1)).thenReturn(DataType.cint());
    when(variables.getType(C2)).thenReturn(DataType.bigint());
    when(variables.getType(C3)).thenReturn(DataType.varchar());

    when(variables.getType(TTL_VARNAME)).thenReturn(DataType.cint());
    when(variables.getType(TIMESTAMP_VARNAME)).thenReturn(DataType.bigint());

    when(record.getFieldValue(F1)).thenReturn("42");
    when(record.getFieldValue(F2)).thenReturn("4242");
    when(record.getFieldValue(F3)).thenReturn("foo");

    when(record.getSource()).thenReturn("source");
    when(record.getLocation()).thenReturn(location);

    when(mapping.fieldToVariable(F1)).thenReturn(C1);
    when(mapping.fieldToVariable(F2)).thenReturn(C2);
    when(mapping.fieldToVariable(F3)).thenReturn(C3);

    when(mapping.codec(C1, DataType.cint(), TypeToken.of(String.class))).thenReturn(codec1);
    when(mapping.codec(C2, DataType.bigint(), TypeToken.of(String.class))).thenReturn(codec2);
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec3);

    when(mapping.codec(TTL_VARNAME, DataType.cint(), TypeToken.of(String.class)))
        .thenReturn(codec1);

    when(mapping.codec(TIMESTAMP_VARNAME, DataType.bigint(), TypeToken.of(String.class)))
        .thenReturn(codec2);
  }

  @Test
  void should_map_regular_fields() throws Exception {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    assertParameter(0, C1, "42", codec1);
    assertParameter(1, C2, "4242", codec2);
    assertParameter(2, C3, "foo", codec3);
  }

  @Test
  void should_bind_mapped_ttl() throws Exception {
    when(record.fields()).thenReturn(set(F1));
    when(mapping.fieldToVariable(F1)).thenReturn(TTL_VARNAME);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    assertParameter(0, TTL_VARNAME, "42", codec1);
  }

  @Test
  void should_bind_mapped_numeric_timestamp() throws Exception {
    when(record.fields()).thenReturn(set(F2));
    when(mapping.fieldToVariable(F2)).thenReturn(TIMESTAMP_VARNAME);
    // timestamp is 123456 minutes before unix epoch
    when(record.getFieldValue(F2)).thenReturn("-123456");
    StringToLongCodec codec =
        spy(new StringToLongCodec(formatter, CQL_DATE_TIME_FORMAT, MINUTES, EPOCH));
    when(mapping.codec(TIMESTAMP_VARNAME, bigint(), TypeToken.of(String.class)))
        .thenReturn((TypeCodec) codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).set(TIMESTAMP_VARNAME, "-123456", codec);
  }

  @Test
  void should_bind_mapped_numeric_timestamp_with_custom_unit_and_epoch() throws Exception {
    when(record.fields()).thenReturn(set(F2));
    when(mapping.fieldToVariable(F2)).thenReturn(TIMESTAMP_VARNAME);
    // timestamp is one minute before year 2000
    when(record.getFieldValue(F2)).thenReturn("-1");
    Instant millennium = Instant.parse("2000-01-01T00:00:00Z");
    StringToLongCodec codec =
        spy(new StringToLongCodec(formatter, CQL_DATE_TIME_FORMAT, MINUTES, millennium));
    when(mapping.codec(TIMESTAMP_VARNAME, bigint(), TypeToken.of(String.class)))
        .thenReturn((TypeCodec) codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).set(TIMESTAMP_VARNAME, "-1", codec);
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp() throws Exception {
    when(record.fields()).thenReturn(set(F2));
    when(mapping.fieldToVariable(F2)).thenReturn(TIMESTAMP_VARNAME);
    when(record.getFieldValue(F2)).thenReturn("2017-01-02T00:00:02");
    StringToLongCodec codec =
        spy(new StringToLongCodec(formatter, CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH));
    when(mapping.codec(TIMESTAMP_VARNAME, bigint(), TypeToken.of(String.class)))
        .thenReturn((TypeCodec) codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).set(TIMESTAMP_VARNAME, "2017-01-02T00:00:02", codec);
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp_with_custom_pattern() throws Exception {
    when(record.fields()).thenReturn(set(F2));
    when(mapping.fieldToVariable(F2)).thenReturn(TIMESTAMP_VARNAME);
    when(record.getFieldValue(F2)).thenReturn("20171123-123456");
    DateTimeFormatter timestampFormat =
        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    StringToLongCodec codec =
        spy(new StringToLongCodec(formatter, timestampFormat, MILLISECONDS, EPOCH));
    when(mapping.codec(TIMESTAMP_VARNAME, bigint(), TypeToken.of(String.class)))
        .thenReturn((TypeCodec) codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).set(TIMESTAMP_VARNAME, "20171123-123456", codec);
  }

  @Test
  void should_map_null_to_unset() throws Exception {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F2)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    assertParameter(0, C1, "42", codec1);
    assertParameter(1, C3, "foo", codec3);
  }

  @Test
  void should_map_null_words_to_unset() throws Exception {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F2)).thenReturn("NIL");
    when(record.getFieldValue(F3)).thenReturn("NULL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of("NIL", "NULL"),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    assertParameter(0, C1, "42", codec1);
  }

  @Test
  void should_map_null_to_null() throws Exception {
    when(record.fields()).thenReturn(set(F1));
    when(record.getFieldValue(F1)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, never())
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo(C1);
  }

  @Test
  void should_map_null_word_to_null() throws Exception {
    when(record.fields()).thenReturn(set(F1));
    when(record.getFieldValue(F1)).thenReturn("NIL");
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of("NIL"),
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, never())
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo(C1);
  }

  @Test
  void should_return_unmappable_statement_when_mapping_fails() throws Exception {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class)))
        .thenThrow(CodecNotFoundException.class);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            ImmutableSet.of(),
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    assertThat(((UnmappableStatement) result).getSource()).isEqualTo(record);
    assertThat(((UnmappableStatement) result).getLocation().toString())
        .isEqualTo(location.toString() + "&field=field3&My+Fancy+Column+Name=foo&cqlType=varchar");
    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());
    assertParameter(0, C1, "42", codec1);
    assertParameter(1, C2, "4242", codec2);
  }

  private void assertParameter(
      int index, String expectedVariable, Object expectedValue, TypeCodec<?> expectedCodec) {
    assertThat(variableCaptor.getAllValues().get(index))
        .isEqualTo(Metadata.quoteIfNecessary(expectedVariable));
    assertThat(valueCaptor.getAllValues().get(index)).isEqualTo(expectedValue);
    assertThat(codecCaptor.getAllValues().get(index)).isSameAs(expectedCodec);
  }

  private static Set<String> set(String... fields) {
    return Sets.newLinkedHashSet(fields);
  }
}
