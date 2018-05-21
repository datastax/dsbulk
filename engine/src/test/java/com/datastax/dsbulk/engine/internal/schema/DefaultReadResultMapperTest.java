/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultReadResultMapperTest {

  private static final String C1 = "col1";
  private static final String C2 = "col2";
  private static final String C3 = "My Fancy Column Name";

  private Mapping mapping;
  private RecordMetadata recordMetadata;
  private ReadResult result;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    recordMetadata =
        new TestRecordMetadata(
            ImmutableMap.of(
                "f0",
                TypeToken.of(Integer.class),
                "f1",
                TypeToken.of(String.class),
                "f2",
                TypeToken.of(String.class)));
    mapping = mock(Mapping.class);
    Row row = mock(Row.class);
    result = mock(ReadResult.class);
    when(result.getRow()).thenReturn(Optional.ofNullable(row));
    ColumnDefinitions.Definition c1 = newDefinition(C1, DataType.cint());
    ColumnDefinitions.Definition c2 = newDefinition(C2, DataType.varchar());
    ColumnDefinitions.Definition c3 = newDefinition(C3, DataType.varchar());
    ColumnDefinitions variables = newColumnDefinitions(c1, c2, c3);
    when(row.getColumnDefinitions()).thenReturn(variables);
    when(mapping.variableToField(C1)).thenReturn("f0");
    when(mapping.variableToField(C2)).thenReturn("f1");
    when(mapping.variableToField(C3)).thenReturn("f2");
    TypeCodec codec1 = TypeCodec.cint();
    TypeCodec codec2 = TypeCodec.varchar();
    when(mapping.codec(C1, DataType.cint(), TypeToken.of(Integer.class))).thenReturn(codec1);
    when(mapping.codec(C2, DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    when(row.get(C1, codec1)).thenReturn(42);
    when(row.get(C2, codec2)).thenReturn("foo");
    when(row.get(C3, codec2)).thenReturn("bar");

    // to generate locations
    BoundStatement boundStatement = mock(BoundStatement.class);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    Host host = mock(Host.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(result.getStatement()).thenReturn(boundStatement);
    when(result.getExecutionInfo()).thenReturn(Optional.ofNullable(executionInfo));
    when(executionInfo.getQueriedHost()).thenReturn(host);
    when(host.getSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
    when(boundStatement.preparedStatement()).thenReturn(ps);
    when(ps.getQueryString()).thenReturn("irrelevant");
    ColumnDefinitions.Definition start = newDefinition("start", DataType.bigint());
    ColumnDefinitions.Definition end = newDefinition("end", DataType.bigint());
    ColumnDefinitions boundVariables = newColumnDefinitions(start, end);
    when(ps.getVariables()).thenReturn(boundVariables);
    when(row.getObject(C1)).thenReturn(42);
    when(row.getObject(C2)).thenReturn("foo");
    when(row.getObject(C3)).thenReturn("bar");
    when(boundStatement.getObject("start")).thenReturn(1234L);
    when(boundStatement.getObject("end")).thenReturn(5678L);
  }

  @Test
  void should_map_result_to_mapped_record_when_mapping_succeeds() {
    DefaultReadResultMapper mapper = new DefaultReadResultMapper(mapping, recordMetadata);
    Record record = mapper.map(result);
    assertThat(record.fields()).containsOnly("f0", "f1", "f2");
    assertThat(record.getFieldValue("f0")).isEqualTo(42);
    assertThat(record.getFieldValue("f1")).isEqualTo("foo");
    assertThat(record.getFieldValue("f2")).isEqualTo("bar");
  }

  @Test
  void should_map_result_to_error_record_when_mapping_fails() {
    CodecNotFoundException exception =
        new CodecNotFoundException("not really", DataType.varchar(), TypeToken.of(String.class));
    when(mapping.codec(C3, DataType.varchar(), TypeToken.of(String.class))).thenThrow(exception);
    DefaultReadResultMapper mapper = new DefaultReadResultMapper(mapping, recordMetadata);
    ErrorRecord record = (ErrorRecord) mapper.map(result);
    assertThat(record.getError()).isSameAs(exception);
    assertThat(record.getSource()).isSameAs(result);
    assertThat(record.getLocation())
        .hasScheme("cql")
        .hasHost("127.0.0.1")
        .hasPort(9042)
        .hasPath("/ks/t")
        .hasParameter("start", "1234")
        .hasParameter("end", "5678")
        .hasParameter(C1, "42")
        .hasParameter(C2, "\'foo\'")
        .hasParameter(C3, "\'bar\'");
  }
}
