/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DriverCoreTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreTestHooks.newDefinition;
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
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecordMetadata;
import com.datastax.dsbulk.engine.internal.record.UnmappableRecord;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class DefaultReadResultMapperTest {

  private Mapping mapping;
  private RecordMetadata recordMetadata;
  private ReadResult result;

  @Before
  public void setUp() throws Exception {
    recordMetadata =
        new DefaultRecordMetadata(
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
    ColumnDefinitions.Definition c1 = newDefinition("c1", DataType.cint());
    ColumnDefinitions.Definition c2 = newDefinition("c2", DataType.varchar());
    ColumnDefinitions.Definition c3 = newDefinition("c3", DataType.varchar());
    ColumnDefinitions variables = newColumnDefinitions(c1, c2, c3);
    when(row.getColumnDefinitions()).thenReturn(variables);
    when(mapping.variableToField("c1")).thenReturn("f0");
    when(mapping.variableToField("c2")).thenReturn("f1");
    when(mapping.variableToField("c3")).thenReturn("f2");
    TypeCodec codec1 = TypeCodec.cint();
    TypeCodec codec2 = TypeCodec.varchar();
    //noinspection unchecked
    when(mapping.codec("c1", DataType.cint(), TypeToken.of(Integer.class))).thenReturn(codec1);
    //noinspection unchecked
    when(mapping.codec("c2", DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    //noinspection unchecked
    when(mapping.codec("c3", DataType.varchar(), TypeToken.of(String.class))).thenReturn(codec2);
    //noinspection unchecked
    when(row.get("c1", codec1)).thenReturn(42);
    //noinspection unchecked
    when(row.get("c2", codec2)).thenReturn("foo");
    //noinspection unchecked
    when(row.get("c3", codec2)).thenReturn("bar");

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
    when(row.getObject("c1")).thenReturn(42);
    when(row.getObject("c2")).thenReturn("foo");
    when(row.getObject("c3")).thenReturn("bar");
    when(boundStatement.getObject("start")).thenReturn(1234L);
    when(boundStatement.getObject("end")).thenReturn(5678L);
  }

  @Test
  public void should_map_result_to_mapped_record_when_mapping_succeeds() throws Exception {
    DefaultReadResultMapper mapper = new DefaultReadResultMapper(mapping, recordMetadata, null);
    Record record = mapper.map(result);
    assertThat(record.fields()).containsOnly("f0", "f1", "f2");
    assertThat(record.getFieldValue("f0")).isEqualTo(42);
    assertThat(record.getFieldValue("f1")).isEqualTo("foo");
    assertThat(record.getFieldValue("f2")).isEqualTo("bar");
  }

  @Test
  public void should_map_result_to_error_record_when_mapping_fails() throws Exception {
    CodecNotFoundException exception =
        new CodecNotFoundException("not really", DataType.varchar(), TypeToken.of(String.class));
    when(mapping.codec("c3", DataType.varchar(), TypeToken.of(String.class))).thenThrow(exception);
    DefaultReadResultMapper mapper = new DefaultReadResultMapper(mapping, recordMetadata, null);
    UnmappableRecord record = (UnmappableRecord) mapper.map(result);
    assertThat(record.getError()).isSameAs(exception);
    assertThat(record.getSource()).isSameAs(result);
    assertThat(record.getLocation())
        .hasScheme("cql")
        .hasHost("127.0.0.1")
        .hasPort(9042)
        .hasPath("/ks/t")
        .hasParameter("start", "1234")
        .hasParameter("end", "5678")
        .hasParameter("c1", "42")
        .hasParameter("c2", "\'foo\'")
        .hasParameter("c3", "\'bar\'");
  }
}
