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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.driver.api.core.ProtocolVersion.V3;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singleton;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.api.format.temporal.CqlTemporalFormat;
import com.datastax.oss.dsbulk.codecs.api.format.temporal.TemporalFormat;
import com.datastax.oss.dsbulk.codecs.api.format.temporal.ZonedTemporalFormat;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.api.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.text.string.StringToIntegerCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToLongCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToStringCodec;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.Mapping;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultRecordMapperTest {

  private static final Field F1 = new DefaultMappedField("field1");
  private static final Field F2 = new DefaultMappedField("field2");
  private static final Field F3 = new DefaultMappedField("field3");

  private static final CQLWord C1 = CQLWord.fromInternal("col1");
  private static final CQLWord C2 = CQLWord.fromInternal("col2");
  private static final CQLWord C3 = CQLWord.fromInternal("My Fancy Column Name");

  private final List<String> nullStrings = newArrayList("");

  private final FastThreadLocal<NumberFormat> formatter =
      CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  @Mock private Mapping mapping;
  @Mock private Record record;
  @Mock private PreparedStatement insertStatement;
  @Mock private BoundStatement boundStatement;
  @Mock private BoundStatementBuilder boundStatementBuilder;
  @Mock private ColumnDefinitions variables;
  @Mock private ColumnDefinition c1Def;
  @Mock private ColumnDefinition c2Def;
  @Mock private ColumnDefinition c3Def;
  @Mock private StringToIntegerCodec codec1;
  @Mock private StringToLongCodec codec2;
  @Mock private StringToStringCodec codec3;

  private ArgumentCaptor<CqlIdentifier> variableCaptor;
  private ArgumentCaptor<ByteBuffer> valueCaptor;

  private RecordMetadata recordMetadata;

  @BeforeEach
  void setUp() {
    variableCaptor = ArgumentCaptor.forClass(CqlIdentifier.class);
    valueCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    recordMetadata =
        new TestRecordMetadata(
            ImmutableMap.of(
                F1, GenericType.STRING, F2, GenericType.STRING, F3, GenericType.STRING));

    when(boundStatementBuilder.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(boundStatementBuilder.build()).thenReturn(boundStatement);

    when(boundStatementBuilder.isSet(0)).thenReturn(true);
    when(boundStatementBuilder.isSet(1)).thenReturn(true);
    when(boundStatementBuilder.isSet(2)).thenReturn(true);

    when(boundStatementBuilder.setBytesUnsafe(any(CqlIdentifier.class), any(ByteBuffer.class)))
        .thenReturn(boundStatementBuilder);
    when(boundStatementBuilder.setBytesUnsafe(any(CqlIdentifier.class), eq(null)))
        .thenReturn(boundStatementBuilder);

    when(insertStatement.getVariableDefinitions()).thenReturn(variables);

    when(variables.get(C1.asIdentifier())).thenReturn(c1Def);
    when(variables.get(C2.asIdentifier())).thenReturn(c2Def);
    when(variables.get(C3.asIdentifier())).thenReturn(c3Def);

    when(variables.get(0)).thenReturn(c1Def);
    when(variables.get(1)).thenReturn(c2Def);
    when(variables.get(2)).thenReturn(c3Def);

    when(c1Def.getType()).thenReturn(DataTypes.INT);
    when(c2Def.getType()).thenReturn(DataTypes.BIGINT);
    when(c3Def.getType()).thenReturn(DataTypes.TEXT);

    when(c1Def.getName()).thenReturn(C1.asIdentifier());
    when(c2Def.getName()).thenReturn(C2.asIdentifier());
    when(c3Def.getName()).thenReturn(C3.asIdentifier());

    when(variables.size()).thenReturn(3);

    when(variables.firstIndexOf(C1.asIdentifier())).thenReturn(0);
    when(variables.firstIndexOf(C2.asIdentifier())).thenReturn(1);
    when(variables.firstIndexOf(C3.asIdentifier())).thenReturn(2);

    when(record.getFieldValue(F1)).thenReturn("42");
    when(record.getFieldValue(F2)).thenReturn("4242");
    when(record.getFieldValue(F3)).thenReturn("foo");

    when(record.getSource()).thenReturn("source");

    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.fields()).thenReturn(set(F1, F2, F3));

    when(mapping.fieldToVariables(F1)).thenReturn(singleton(C1));
    when(mapping.fieldToVariables(F2)).thenReturn(singleton(C2));
    when(mapping.fieldToVariables(F3)).thenReturn(singleton(C3));

    when(mapping.variableToFields(C1)).thenReturn(singleton(F1));
    when(mapping.variableToFields(C2)).thenReturn(singleton(F2));
    when(mapping.variableToFields(C3)).thenReturn(singleton(F3));

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
    when(codec3.encode(any(), any()))
        .thenAnswer(
            invocation -> {
              String s = invocation.getArgument(0);
              if (s == null) {
                return null;
              }
              return TypeCodecs.TEXT.encode(s, invocation.getArgument(1));
            });
  }

  @Test
  void should_map_regular_fields() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
    assertParameter(2, C3, TypeCodecs.TEXT.encode("foo", V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(c1Def.getType()).thenReturn(DataTypes.BIGINT);
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
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder)
        .setBytesUnsafe(C1.asIdentifier(), TypeCodecs.BIGINT.encode(-123456L, V4));
  }

  @Test
  void should_bind_mapped_numeric_timestamp_with_custom_unit_and_epoch() {
    when(record.fields()).thenReturn(set(F1));
    when(c1Def.getType()).thenReturn(DataTypes.BIGINT);
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
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder)
        .setBytesUnsafe(C1.asIdentifier(), TypeCodecs.BIGINT.encode(-1L, V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp() {
    when(record.fields()).thenReturn(set(F1));
    when(c1Def.getType()).thenReturn(DataTypes.BIGINT);
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
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder)
        .setBytesUnsafe(
            C1.asIdentifier(),
            TypeCodecs.BIGINT.encode(Instant.parse("2017-01-02T00:00:02Z").toEpochMilli(), V4));
  }

  @Test
  void should_bind_mapped_alphanumeric_timestamp_with_custom_pattern() {
    when(record.fields()).thenReturn(set(F1));
    when(c1Def.getType()).thenReturn(DataTypes.BIGINT);
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
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder)
        .setBytesUnsafe(
            C1.asIdentifier(),
            TypeCodecs.BIGINT.encode(Instant.parse("2017-11-23T12:34:56Z").toEpochMilli(), V4));
  }

  @Test
  void should_map_null_to_unset() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F2)).thenReturn(null);
    when(insertStatement.getPartitionKeyIndices()).thenReturn(Arrays.asList(0, 2));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C3),
            V4,
            mapping,
            recordMetadata,
            true,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(2))
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
            Collections.singletonList(insertStatement),
            set(C2),
            set(C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder).setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, null);
  }

  @Test
  void should_set_unmapped_to_null_when_protocol_v3() {
    when(record.fields()).thenReturn(set(F1, F2));
    when(boundStatementBuilder.isSet(2)).thenReturn(false);
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2),
            V3,
            mapping,
            recordMetadata,
            false,
            true,
            true,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
    verify(boundStatementBuilder).setToNull(2);
  }

  @Test
  void should_return_unmappable_statement_when_mapping_fails() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(mapping.codec(C3, DataTypes.TEXT, GenericType.STRING))
        .thenThrow(CodecNotFoundException.class);
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    assertThat(((UnmappableStatement) result).getRecord()).isEqualTo(record);
    verify(boundStatementBuilder, times(2))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_null() {
    when(record.getFieldValue(F1)).thenReturn(null);
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be set to null");
  }

  @Test
  void should_return_unmappable_statement_when_pk_column_unmapped() {
    when(boundStatementBuilder.allIndicesOf(C1.asIdentifier()))
        .thenReturn(Collections.singletonList(0));
    when(boundStatementBuilder.isSet(0)).thenReturn(false);
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be left unset");
  }

  @Test
  void should_return_unmappable_statement_when_extra_field() {
    when(mapping.fields()).thenReturn(set(F1, F2));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            false,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
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
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining(
            "Required field field3 (mapped to column \"My Fancy Column Name\") was missing from record. "
                + "Please remove it from the mapping "
                + "or set schema.allowMissingFields to true.");
  }

  @Test
  void should_map_when_pk_column_is_empty_blob() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(c1Def.getType()).thenReturn(DataTypes.TEXT);
    when(c2Def.getType()).thenReturn(DataTypes.ASCII);
    when(c3Def.getType()).thenReturn(DataTypes.BLOB);
    when(record.getFieldValue(F1)).thenReturn("foo");
    when(record.getFieldValue(F2)).thenReturn("foo");
    when(record.getFieldValue(F3)).thenReturn(""); // blobs can be empty
    when(mapping.codec(C1, DataTypes.TEXT, GenericType.STRING)).thenReturn(codec1);
    when(mapping.codec(C2, DataTypes.ASCII, GenericType.STRING)).thenReturn(codec2);
    when(mapping.codec(C3, DataTypes.BLOB, GenericType.STRING)).thenReturn(codec3);
    when(codec1.encode(any(), any())).thenReturn(ByteBuffer.wrap("foo".getBytes(UTF_8)));
    when(codec2.encode(any(), any())).thenReturn(ByteBuffer.wrap("foo".getBytes(UTF_8)));
    when(codec3.encode(any(), any())).thenReturn(ByteBuffer.allocate(0));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, ByteBuffer.wrap("foo".getBytes(UTF_8)));
    assertParameter(1, C2, ByteBuffer.wrap("foo".getBytes(UTF_8)));
    assertParameter(2, C3, ByteBuffer.allocate(0));
  }

  @Test
  void should_not_map_when_partition_key_column_is_empty_string() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F1)).thenReturn("");
    when(c1Def.getType()).thenReturn(DataTypes.TEXT);
    when(mapping.codec(C1, DataTypes.TEXT, GenericType.STRING)).thenReturn(codec1);
    when(codec1.encode(any(), any())).thenReturn(ByteBuffer.allocate(0));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be set to empty");
  }

  @Test
  void should_not_map_when_partition_key_column_is_empty_blob() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(c1Def.getType()).thenReturn(DataTypes.BLOB);
    when(record.getFieldValue(F1)).thenReturn("");
    when(mapping.codec(C1, DataTypes.BLOB, GenericType.STRING)).thenReturn(codec1);
    when(codec1.encode(any(), any())).thenReturn(ByteBuffer.allocate(0));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isNotSameAs(boundStatement).isInstanceOf(UnmappableStatement.class);
    UnmappableStatement unmappableStatement = (UnmappableStatement) result;
    assertThat(unmappableStatement.getError())
        .isInstanceOf(InvalidMappingException.class)
        .hasMessageContaining("Primary key column col1 cannot be set to empty");
  }

  @Test
  void should_map_when_clustering_column_is_empty_string() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(record.getFieldValue(F3)).thenReturn("");
    when(codec3.encode(any(), any())).thenReturn(ByteBuffer.allocate(0));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
    assertParameter(2, C3, ByteBuffer.allocate(0));
  }

  @Test
  void should_map_when_clustering_column_is_empty_blob() {
    when(record.fields()).thenReturn(set(F1, F2, F3));
    when(c3Def.getType()).thenReturn(DataTypes.BLOB);
    when(record.getFieldValue(F3)).thenReturn("");
    when(mapping.codec(C3, DataTypes.BLOB, GenericType.STRING)).thenReturn(codec3);
    when(codec3.encode(any(), any())).thenReturn(ByteBuffer.allocate(0));
    RecordMapper mapper =
        new DefaultRecordMapper(
            Collections.singletonList(insertStatement),
            set(C1),
            set(C2, C3),
            V4,
            mapping,
            recordMetadata,
            false,
            true,
            false,
            statement -> boundStatementBuilder);
    Statement<?> result = mapper.map(record).single().block();
    assertThat(result).isInstanceOf(MappedBoundStatement.class);
    assertThat(ReflectionUtils.getInternalState(result, "delegate")).isSameAs(boundStatement);
    verify(boundStatementBuilder, times(3))
        .setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertParameter(0, C1, TypeCodecs.INT.encode(42, V4));
    assertParameter(1, C2, TypeCodecs.BIGINT.encode(4242L, V4));
    assertParameter(2, C3, ByteBuffer.allocate(0));
  }

  @Test
  void should_map_to_multiple_statements() {
    when(record.fields()).thenReturn(set(F1, F2, F3));

    PreparedStatement ps1 = mock(PreparedStatement.class);
    PreparedStatement ps2 = mock(PreparedStatement.class);
    ColumnDefinitions vars1 = mock(ColumnDefinitions.class);
    ColumnDefinitions vars2 = mock(ColumnDefinitions.class);
    when(ps1.getVariableDefinitions()).thenReturn(vars1);
    when(ps2.getVariableDefinitions()).thenReturn(vars2);
    when(vars1.contains(C1.asIdentifier())).thenReturn(true);
    when(vars1.contains(C2.asIdentifier())).thenReturn(true);
    when(vars2.contains(C1.asIdentifier())).thenReturn(true);
    when(vars2.contains(C3.asIdentifier())).thenReturn(true);
    when(vars1.get(C1.asIdentifier())).thenReturn(c1Def);
    when(vars1.get(C2.asIdentifier())).thenReturn(c2Def);
    when(vars2.get(C1.asIdentifier())).thenReturn(c1Def);
    when(vars2.get(C3.asIdentifier())).thenReturn(c3Def);
    when(vars1.get(0)).thenReturn(c1Def);
    when(vars1.get(1)).thenReturn(c2Def);
    when(vars1.get(0)).thenReturn(c1Def);
    when(vars1.get(1)).thenReturn(c3Def);

    BoundStatement bs1 = mock(BoundStatement.class);
    BoundStatement bs2 = mock(BoundStatement.class);

    BoundStatementBuilder bsb1 = mock(BoundStatementBuilder.class);
    when(bsb1.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(bsb1.build()).thenReturn(bs1);
    when(bsb1.setBytesUnsafe(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(bsb1);
    when(bsb1.setBytesUnsafe(any(CqlIdentifier.class), eq(null))).thenReturn(bsb1);

    BoundStatementBuilder bsb2 = mock(BoundStatementBuilder.class);
    when(bsb2.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(bsb2.build()).thenReturn(bs2);
    when(bsb2.setBytesUnsafe(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(bsb2);
    when(bsb2.setBytesUnsafe(any(CqlIdentifier.class), eq(null))).thenReturn(bsb2);

    RecordMapper mapper =
        new DefaultRecordMapper(
            ImmutableList.of(ps1, ps2),
            set(C1),
            Collections.emptySet(),
            V4,
            mapping,
            recordMetadata,
            true,
            false,
            false,
            statement -> statement == ps1 ? bsb1 : bsb2);
    List<BatchableStatement<?>> results = mapper.map(record).collectList().block();
    assertThat(results)
        .hasSize(2)
        .allSatisfy(stmt -> assertThat(stmt).isInstanceOf(MappedBoundStatement.class));

    ArgumentCaptor<CqlIdentifier> variableCaptor = ArgumentCaptor.forClass(CqlIdentifier.class);
    ArgumentCaptor<ByteBuffer> valueCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(bsb1, times(2)).setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertThat(variableCaptor.getAllValues().get(0)).isEqualTo(C1.asIdentifier());
    assertThat(valueCaptor.getAllValues().get(0)).isEqualTo(TypeCodecs.INT.encode(42, V4));
    assertThat(variableCaptor.getAllValues().get(1)).isEqualTo(C2.asIdentifier());
    assertThat(valueCaptor.getAllValues().get(1)).isEqualTo(TypeCodecs.BIGINT.encode(4242L, V4));

    variableCaptor = ArgumentCaptor.forClass(CqlIdentifier.class);
    valueCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(bsb2, times(2)).setBytesUnsafe(variableCaptor.capture(), valueCaptor.capture());
    assertThat(variableCaptor.getAllValues().get(0)).isEqualTo(C1.asIdentifier());
    assertThat(valueCaptor.getAllValues().get(0)).isEqualTo(TypeCodecs.INT.encode(42, V4));
    assertThat(variableCaptor.getAllValues().get(1)).isEqualTo(C3.asIdentifier());
    assertThat(valueCaptor.getAllValues().get(1)).isEqualTo(TypeCodecs.TEXT.encode("foo", V4));
  }

  private void assertParameter(
      int invocationIndex, CQLWord expectedVariable, ByteBuffer expectedVariableValue) {
    assertThat(variableCaptor.getAllValues().get(invocationIndex))
        .isEqualTo(expectedVariable.asIdentifier());
    assertThat(valueCaptor.getAllValues().get(invocationIndex)).isEqualTo(expectedVariableValue);
  }

  @SafeVarargs
  private static <T> Set<T> set(T... elements) {
    return Sets.newLinkedHashSet(elements);
  }
}
