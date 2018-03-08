/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.netty.util.concurrent.FastThreadLocal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
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

  private final TypeCodec codec1 = mock(StringToIntegerCodec.class);
  private final TypeCodec codec2 = mock(StringToLongCodec.class);
  private final TypeCodec codec3 = TypeCodec.varchar();

  private final int[] pkIndices = {0, 1, 2};

  private final List<String> nullWords = newArrayList("");

  private Mapping mapping;
  private Record record;
  private PreparedStatement insertStatement;
  private BoundStatement boundStatement;
  private ColumnDefinitions variables;
  private ArgumentCaptor<String> variableCaptor;
  private ArgumentCaptor<ByteBuffer> valueCaptor;
  private RecordMetadata recordMetadata;
  private final FastThreadLocal<NumberFormat> formatter =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  @BeforeEach
  void setUp() {
    variableCaptor = ArgumentCaptor.forClass(String.class);
    valueCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

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
    variables = mock(ColumnDefinitions.class);

    when(boundStatement.preparedStatement()).thenReturn(insertStatement);
    when(boundStatement.isSet(0)).thenReturn(true);
    when(boundStatement.isSet(1)).thenReturn(true);
    when(boundStatement.isSet(2)).thenReturn(true);
    when(insertStatement.getVariables()).thenReturn(variables);

    when(variables.getType(C1)).thenReturn(DataType.cint());
    when(variables.getType(C2)).thenReturn(DataType.bigint());
    when(variables.getType(C3)).thenReturn(DataType.varchar());
    when(variables.getIndexOf(C1)).thenReturn(0);
    when(variables.getIndexOf(C2)).thenReturn(1);
    when(variables.getIndexOf(C3)).thenReturn(2);
    when(variables.getName(0)).thenReturn(C1);
    when(variables.getName(1)).thenReturn(C2);
    when(variables.getName(2)).thenReturn(C3);

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

    // emulate the behavior of a ConvertingCodec (StringToXCodec)
    when(codec1.serialize(any(), any()))
        .thenAnswer(
            invocation -> {
              String s = invocation.getArgument(0);
              if (s == null) {
                return null;
              }
              return TypeCodec.cint().serialize(Integer.parseInt(s), invocation.getArgument(1));
            });
    when(codec2.serialize(any(), any()))
        .thenAnswer(
            invocation -> {
              String s = invocation.getArgument(0);
              if (s == null) {
                return null;
              }
              return TypeCodec.bigint().serialize(Long.parseLong(s), invocation.getArgument(1));
            });
  }

  @Test
  void should_map_regular_fields() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodec.cint().serialize(42, V4));
    assertParameter(1, C2, TypeCodec.bigint().serialize(4242L, V4));
    assertParameter(2, C3, TypeCodec.varchar().serialize("foo", V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.getType(C1)).thenReturn(bigint());
    // timestamp is 123456 minutes before unix epoch
    when(record.getFieldValue(F1)).thenReturn("-123456");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CQL_DATE_TIME_FORMAT,
                MINUTES,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullWords));
    when(mapping.codec(C1, bigint(), TypeToken.of(String.class))).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(C1, TypeCodec.bigint().serialize(-123456L, V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp_with_custom_unit_and_epoch() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.getType(C1)).thenReturn(bigint());
    // timestamp is one minute before year 2000
    when(record.getFieldValue(F1)).thenReturn("-1");
    Instant millennium = Instant.parse("2000-01-01T00:00:00Z");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CQL_DATE_TIME_FORMAT,
                MINUTES,
                millennium.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullWords));
    when(mapping.codec(C1, bigint(), TypeToken.of(String.class))).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(C1, TypeCodec.bigint().serialize(-1L, V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.getType(C1)).thenReturn(bigint());
    when(record.getFieldValue(F1)).thenReturn("2017-01-02T00:00:02");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullWords));
    when(mapping.codec(C1, bigint(), TypeToken.of(String.class))).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .setBytesUnsafe(
            C1,
            TypeCodec.bigint().serialize(Instant.parse("2017-01-02T00:00:02Z").toEpochMilli(), V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp_with_custom_pattern() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.getType(C1)).thenReturn(bigint());
    when(record.getFieldValue(F1)).thenReturn("20171123-123456");
    DateTimeFormatter timestampFormat =
        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                timestampFormat,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullWords));
    when(mapping.codec(C1, bigint(), TypeToken.of(String.class))).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .setBytesUnsafe(
            C1,
            TypeCodec.bigint().serialize(Instant.parse("2017-11-23T12:34:56Z").toEpochMilli(), V4));
  }

  @Test
  void should_map_null_to_unset() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F2)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            new int[] {0, 2},
            V4,
            mapping,
            recordMetadata,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodec.cint().serialize(42, V4));
    assertParameter(1, C3, TypeCodec.varchar().serialize("foo", V4));
  }

  @Test
  void should_map_null_to_null() {
    when(record.fields()).thenReturn(set(F1));
    when(record.getFieldValue(F1)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            new int[] {1, 2},
            V4,
            mapping,
            recordMetadata,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, null);
  }

  @Test
  void should_return_unmappable_statement_when_mapping_fails() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class)))
        .thenThrow(CodecNotFoundException.class);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    assertThat(((UnmappableStatement) result).getSource()).isEqualTo(record);
    assertThat(((UnmappableStatement) result).getLocation().toString())
        .isEqualTo(location.toString() + "&field=field3&My+Fancy+Column+Name=foo&cqlType=varchar");
    verify(boundStatement, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodec.cint().serialize(42, V4));
    assertParameter(1, C2, TypeCodec.bigint().serialize(4242L, V4));
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_null() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F1)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Primary key column col1 cannot be mapped to null");
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_unmapped() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(variables.getIndexOf(C1)).thenReturn(3);
    when(variables.getName(3)).thenReturn(C1);
    when(boundStatement.isSet(0)).thenReturn(false);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            pkIndices,
            V4,
            mapping,
            recordMetadata,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Primary key column col1 cannot be left unmapped");
  }

  private void assertParameter(int index, String expectedVariable, ByteBuffer expectedValue) {
    assertThat(variableCaptor.getAllValues().get(index))
        .isEqualTo(Metadata.quoteIfNecessary(expectedVariable));
    assertThat(valueCaptor.getAllValues().get(index)).isEqualTo(expectedValue);
  }

  private static Set<String> set(String... fields) {
    return Sets.newLinkedHashSet(fields);
  }
}
