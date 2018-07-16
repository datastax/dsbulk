/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
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

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.commons.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.commons.codecs.util.CqlTemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.ZonedTemporalFormat;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
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

  private static final CqlIdentifier C1 = CqlIdentifier.fromInternal("col1");
  private static final CqlIdentifier C2 = CqlIdentifier.fromInternal("col2");
  private static final CqlIdentifier C3 = CqlIdentifier.fromInternal("My Fancy Column Name");

  private final URI location = URI.create("file://file1?line=1");

  private final TypeCodec codec1 = mock(StringToIntegerCodec.class);
  private final TypeCodec codec2 = mock(StringToLongCodec.class);
  private final TypeCodec codec3 = TypeCodecs.TEXT;

  private final List<Integer> pkIndices = Arrays.asList(0, 1, 2);

  private final List<String> nullStrings = newArrayList("");

  private Mapping mapping;
  private Record record;
  private PreparedStatement insertStatement;
  private BoundStatement boundStatement;
  private ColumnDefinitions variables;
  private ArgumentCaptor<CqlIdentifier> variableCaptor;
  private ArgumentCaptor<ByteBuffer> valueCaptor;
  private RecordMetadata recordMetadata;
  private final FastThreadLocal<NumberFormat> formatter =
      CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  @BeforeEach
  void setUp() {
    variableCaptor = ArgumentCaptor.forClass(CqlIdentifier.class);
    valueCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    recordMetadata =
        new TestRecordMetadata(
            ImmutableMap.of(
                F1, GenericType.STRING, F2, GenericType.STRING, F3, GenericType.STRING));

    boundStatement = mock(BoundStatement.class);
    mapping = mock(Mapping.class);
    record = mock(Record.class);
    insertStatement = mock(PreparedStatement.class);
    variables = mock(ColumnDefinitions.class);

    when(boundStatement.getPreparedStatement()).thenReturn(insertStatement);
    when(boundStatement.protocolVersion()).thenReturn(V4);
    when(boundStatement.isSet(0)).thenReturn(true);
    when(boundStatement.isSet(1)).thenReturn(true);
    when(boundStatement.isSet(2)).thenReturn(true);
    when(insertStatement.getVariableDefinitions()).thenReturn(variables);
    when(insertStatement.getPartitionKeyIndices()).thenReturn(pkIndices);
    ColumnDefinition c1Def = mock(ColumnDefinition.class);
    ColumnDefinition c2Def = mock(ColumnDefinition.class);
    ColumnDefinition c3Def = mock(ColumnDefinition.class);
    when(variables.get(C1)).thenReturn(c1Def);
    when(variables.get(C2)).thenReturn(c2Def);
    when(variables.get(C3)).thenReturn(c3Def);

    when(c1Def.getType()).thenReturn(DataTypes.INT);
    when(c2Def.getType()).thenReturn(DataTypes.BIGINT);
    when(c3Def.getType()).thenReturn(DataTypes.TEXT);
    when(variables.firstIndexOf(C1)).thenReturn(0);
    when(variables.firstIndexOf(C2)).thenReturn(1);
    when(variables.firstIndexOf(C3)).thenReturn(2);

    when(variables.get(0)).thenReturn(c1Def);
    when(variables.get(1)).thenReturn(c2Def);
    when(variables.get(2)).thenReturn(c3Def);

    when(c1Def.getName()).thenReturn(C1);
    when(c2Def.getName()).thenReturn(C2);
    when(c3Def.getName()).thenReturn(C3);
    when(variables.size()).thenReturn(3);

    when(record.getFieldValue(F1)).thenReturn("42");
    when(record.getFieldValue(F2)).thenReturn("4242");
    when(record.getFieldValue(F3)).thenReturn("foo");

    when(record.getSource()).thenReturn("source");
    when(record.getLocation()).thenReturn(location);

    when(mapping.fieldToVariable(F1)).thenReturn(C1);
    when(mapping.fieldToVariable(F2)).thenReturn(C2);
    when(mapping.fieldToVariable(F3)).thenReturn(C3);

    when(mapping.variableToField(C1)).thenReturn(F1);
    when(mapping.variableToField(C2)).thenReturn(F2);
    when(mapping.variableToField(C3)).thenReturn(F3);

    when(mapping.codec(C1, DataTypes.INT, GenericType.STRING)).thenReturn(codec1);
    when(mapping.codec(C2, DataTypes.BIGINT, GenericType.STRING)).thenReturn(codec2);
    when(mapping.codec(C3, DataTypes.TEXT, GenericType.STRING)).thenReturn(codec3);

    // emulate the behavior of a ConvertingCodec (StringToXCodec)
    when(codec1.encode(any(), any()))
        .thenAnswer(
            invocation -> {
              String s = invocation.getArgument(0);
              if (s == null) {
                return null;
              }
              return TypeCodecs.INT.encode(Integer.parseInt(s), invocation.getArgument(1));
            });
    when(codec2.encode(any(), any()))
        .thenAnswer(
            invocation -> {
              String s = invocation.getArgument(0);
              if (s == null) {
                return null;
              }
              return TypeCodecs.BIGINT.encode(Long.parseLong(s), invocation.getArgument(1));
            });
  }

  @Test
  void should_map_regular_fields() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
    assertParameter(2, C3, TypeCodecs.TEXT.encode("foo", V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.get(C1).getType()).thenReturn(DataTypes.BIGINT);
    // timestamp is 123456 minutes before unix epoch
    when(record.getFieldValue(F1)).thenReturn("-123456");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                TypeCodecs.BIGINT,
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CqlTemporalFormat.DEFAULT_INSTANCE,
                UTC,
                MINUTES,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullStrings));
    when(mapping.codec(C1, DataTypes.BIGINT, GenericType.STRING)).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(C1, TypeCodecs.BIGINT.encode(-123456L, V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp_with_custom_unit_and_epoch() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.get(C1).getType()).thenReturn(DataTypes.BIGINT);
    // timestamp is one minute before year 2000
    when(record.getFieldValue(F1)).thenReturn("-1");
    Instant millennium = Instant.parse("2000-01-01T00:00:00Z");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                TypeCodecs.BIGINT,
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CqlTemporalFormat.DEFAULT_INSTANCE,
                UTC,
                MINUTES,
                millennium.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullStrings));
    when(mapping.codec(C1, DataTypes.BIGINT, GenericType.STRING)).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(C1, TypeCodecs.BIGINT.encode(-1L, V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.get(C1).getType()).thenReturn(DataTypes.BIGINT);
    when(record.getFieldValue(F1)).thenReturn("2017-01-02T00:00:02");
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                TypeCodecs.BIGINT,
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                CqlTemporalFormat.DEFAULT_INSTANCE,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullStrings));
    when(mapping.codec(C1, DataTypes.BIGINT, GenericType.STRING)).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .setBytesUnsafe(
            C1, TypeCodecs.BIGINT.encode(Instant.parse("2017-01-02T00:00:02Z").toEpochMilli(), V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp_with_custom_pattern() {
    when(record.fields()).thenReturn(set(F1));
    when(variables.get(C1).getType()).thenReturn(DataTypes.BIGINT);
    when(record.getFieldValue(F1)).thenReturn("20171123-123456");
    TemporalFormat timestampFormat =
        new ZonedTemporalFormat(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"), UTC);
    StringToLongCodec codec =
        spy(
            new StringToLongCodec(
                TypeCodecs.BIGINT,
                formatter,
                OverflowStrategy.REJECT,
                HALF_EVEN,
                timestampFormat,
                UTC,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                ImmutableMap.of("true", true, "false", false),
                newArrayList(ONE, ZERO),
                nullStrings));
    when(mapping.codec(C1, DataTypes.BIGINT, GenericType.STRING)).thenReturn(codec);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement)
        .setBytesUnsafe(
            C1, TypeCodecs.BIGINT.encode(Instant.parse("2017-11-23T12:34:56Z").toEpochMilli(), V4));
  }

  @Test
  void should_map_null_to_unset() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F2)).thenReturn(null);
    when(insertStatement.getPartitionKeyIndices()).thenReturn(Arrays.asList(0, 2));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            true,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C3, TypeCodecs.TEXT.encode("foo", V4));
  }

  @Test
  void should_map_null_to_null() {
    when(record.fields()).thenReturn(set(F1));
    when(record.getFieldValue(F1)).thenReturn(null);
    when(insertStatement.getPartitionKeyIndices()).thenReturn(Arrays.asList(1, 2));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            true,
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);
    verify(boundStatement).setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, null);
  }

  @Test
  void should_return_unmappable_statement_when_mapping_fails() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.codec(C3, DataTypes.TEXT, GenericType.STRING))
        .thenThrow(CodecNotFoundException.class);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    assertThat(((UnmappableStatement) result).getSource()).isEqualTo(record);
    assertThat(((UnmappableStatement) result).getLocation().toString())
        .isEqualTo(location.toString() + "&field=field3&My+Fancy+Column+Name=foo&cqlType=TEXT");
    verify(boundStatement, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_null() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F1)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be mapped to null");
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_unmapped() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(variables.firstIndexOf(C1)).thenReturn(3);
    ColumnDefinition def = mock(ColumnDefinition.class);
    when(variables.get(3)).thenReturn(def);
    when(def.getName()).thenReturn(C1);
    when(boundStatement.isSet(0)).thenReturn(false);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be left unmapped");
  }

  @Test
  void should_return_unmappable_statement_when_extra_field() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.fieldToVariable(F3)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            false,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining(
            "Extraneous field field3 was found in record. "
                + "Please declare it explicitly in the mapping "
                + "or set schema.allowExtraFields to true.");
  }

  @Test
  void should_return_unmappable_statement_when_missing_field() {
    when(record.fields()).thenReturn(set(F1, F2));
    RecordMapper mapper =
        new DefaultRecordMapper(
            insertStatement,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining(
            "Required field field3 (mapped to column \"My Fancy Column Name\") was missing from record. "
                + "Please remove it from the mapping "
                + "or set schema.allowMissingFields to true.");
  }

  private void assertParameter(
      int index, CqlIdentifier expectedVariable, ByteBuffer expectedValue) {
    assertThat(variableCaptor.getAllValues().get(index)).isEqualTo(expectedVariable);
    assertThat(valueCaptor.getAllValues().get(index)).isEqualTo(expectedValue);
  }

  private static Set<String> set(String... fields) {
    return Sets.newLinkedHashSet(fields);
  }
}
