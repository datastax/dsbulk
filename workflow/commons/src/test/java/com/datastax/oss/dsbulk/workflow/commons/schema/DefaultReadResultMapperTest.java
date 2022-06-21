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

import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockColumnDefinition;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockColumnDefinitions;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Sets.newLinkedHashSet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.Mapping;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatement;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DefaultReadResultMapperTest {

  private static final CQLWord C1 = CQLWord.fromInternal("col1");
  private static final CQLWord C2 = CQLWord.fromInternal("col2");
  private static final CQLWord C3 = CQLWord.fromInternal("My Fancy Column Name");

  private static final DefaultMappedField F0 = new DefaultMappedField("f0");
  private static final DefaultMappedField F1 = new DefaultMappedField("f1");
  private static final DefaultMappedField F2 = new DefaultMappedField("f2");

  private static final URI RESOURCE = URI.create("cql://ks1/table1?start=1&end=2");

  private Mapping mapping;
  private RecordMetadata recordMetadata;
  private ReadResult result;
  private Row row;
  private TypeCodec<Integer> codec1;

  @BeforeEach
  void setUp() {
    recordMetadata =
        new TestRecordMetadata(
            ImmutableMap.of(
                F0,
                GenericType.of(Integer.class),
                F1,
                GenericType.of(String.class),
                F2,
                GenericType.of(String.class)));
    mapping = mock(Mapping.class);
    row = mock(Row.class);
    when(row.codecRegistry()).thenReturn(new DefaultCodecRegistry("test"));
    result = mock(ReadResult.class);
    when(result.getRow()).thenReturn(Optional.of(row));
    ColumnDefinition c1 = mockColumnDefinition(C1.asIdentifier(), DataTypes.INT);
    ColumnDefinition c2 = mockColumnDefinition(C2.asIdentifier(), DataTypes.TEXT);
    ColumnDefinition c3 = mockColumnDefinition(C3.asIdentifier(), DataTypes.TEXT);
    ColumnDefinitions variables = mockColumnDefinitions(c1, c2, c3);
    when(row.getColumnDefinitions()).thenReturn(variables);
    when(mapping.fields()).thenReturn(newLinkedHashSet(F0, F1, F2));
    when(mapping.fieldToVariables(F0)).thenReturn(singleton(C1));
    when(mapping.fieldToVariables(F1)).thenReturn(singleton(C2));
    when(mapping.fieldToVariables(F2)).thenReturn(singleton(C3));
    when(mapping.variableToFields(C1)).thenReturn(singleton(F0));
    when(mapping.variableToFields(C2)).thenReturn(singleton(F1));
    when(mapping.variableToFields(C3)).thenReturn(singleton(F2));
    codec1 = TypeCodecs.INT;
    TypeCodec<String> codec2 = TypeCodecs.TEXT;
    when(mapping.codec(C1, DataTypes.INT, GenericType.of(Integer.class))).thenReturn(codec1);
    when(mapping.codec(C2, DataTypes.TEXT, GenericType.of(String.class))).thenReturn(codec2);
    when(mapping.codec(C3, DataTypes.TEXT, GenericType.of(String.class))).thenReturn(codec2);
    when(row.get(C1.asIdentifier(), codec1)).thenReturn(42);
    when(row.get(C2.asIdentifier(), codec2)).thenReturn("foo");
    when(row.get(C3.asIdentifier(), codec2)).thenReturn("bar");

    // to generate locations
    RangeReadBoundStatement boundStatement = mock(RangeReadBoundStatement.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(result.getStatement()).then(args -> boundStatement);
    when(boundStatement.getPreparedStatement()).thenReturn(ps);
    when(boundStatement.getResource()).thenReturn(RESOURCE);
    when(ps.getQuery()).thenReturn("irrelevant");
    ColumnDefinition start = mockColumnDefinition("start", DataTypes.BIGINT);
    ColumnDefinition end = mockColumnDefinition("end", DataTypes.BIGINT);
    ColumnDefinitions boundVariables = mockColumnDefinitions(start, end);
    when(ps.getVariableDefinitions()).thenReturn(boundVariables);
    when(row.getObject(C1.asIdentifier())).thenReturn(42);
    when(row.getObject(C2.asIdentifier())).thenReturn("foo");
    when(row.getObject(C3.asIdentifier())).thenReturn("bar");
    when(boundStatement.getObject(CqlIdentifier.fromInternal("start"))).thenReturn(1234L);
    when(boundStatement.getObject(CqlIdentifier.fromInternal("end"))).thenReturn(5678L);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_map_result_to_mapped_record_when_mapping_succeeds(boolean retainRecordSources) {
    DefaultReadResultMapper mapper =
        new DefaultReadResultMapper(mapping, recordMetadata, retainRecordSources);
    Record record = mapper.map(result);
    Assertions.assertThat(record.fields()).containsOnly(F0, F1, F2);
    assertThat(record.getFieldValue(F0)).isEqualTo(42);
    assertThat(record.getFieldValue(F1)).isEqualTo("foo");
    assertThat(record.getFieldValue(F2)).isEqualTo("bar");
    if (retainRecordSources) {
      assertThat(record.getSource()).isSameAs(result);
    } else {
      assertThat(record.getSource()).isNull();
    }
    assertThat(record.getResource()).isEqualTo(RESOURCE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_map_result_to_error_record_when_mapping_fails(boolean retainRecordSources) {
    // emulate a bad mapping (bad writetime variable) - see DefaultMapping
    String msg = "Cannot create a WriteTimeCodec for int";
    IllegalArgumentException error = new IllegalArgumentException(msg);
    when(mapping.codec(C1, DataTypes.INT, GenericType.INTEGER)).thenThrow(error);
    DefaultReadResultMapper mapper =
        new DefaultReadResultMapper(mapping, recordMetadata, retainRecordSources);
    ErrorRecord record = (ErrorRecord) mapper.map(result);
    assertThat(record.getError())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Could not deserialize column col1 of type INT as java.lang.Integer")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    Throwable cause = record.getError().getCause();
    assertThat(cause).hasMessage(msg);
    if (retainRecordSources) {
      assertThat(record.getSource()).isSameAs(result);
    } else {
      assertThat(record.getSource()).isNull();
    }
    assertThat(record.getResource()).isEqualTo(RESOURCE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_map_result_to_error_record_when_deser_fails(boolean retainRecordSources) {
    // emulate bad byte buffer contents when deserializing a 4-byte integer
    String msg = "Invalid 32-bits integer value, expecting 4 bytes but got 5";
    IllegalArgumentException error = new IllegalArgumentException(msg);
    when(row.get(C1.asIdentifier(), codec1)).thenThrow(error);
    byte[] array = {1, 2, 3, 4, 5};
    when(row.getBytesUnsafe(C1.asIdentifier())).thenReturn(ByteBuffer.wrap(array));
    DefaultReadResultMapper mapper =
        new DefaultReadResultMapper(mapping, recordMetadata, retainRecordSources);
    ErrorRecord record = (ErrorRecord) mapper.map(result);
    assertThat(record.getError())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Could not deserialize column col1 of type INT as java.lang.Integer")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    Throwable cause = record.getError().getCause();
    assertThat(cause).hasMessage(msg);
    if (retainRecordSources) {
      assertThat(record.getSource()).isSameAs(result);
    } else {
      assertThat(record.getSource()).isNull();
    }
    assertThat(record.getResource()).isEqualTo(RESOURCE);
  }
}
