/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

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
import com.datastax.loader.executor.api.result.ReadResult;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class ResultUtilsTest {

  private ReadResult result;

  @Before
  public void setUp() throws Exception {
    result = mock(ReadResult.class);
    BoundStatement boundStatement = mock(BoundStatement.class);
    Row row = mock(Row.class);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    Host host = mock(Host.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(result.getRow()).thenReturn(Optional.ofNullable(row));
    when(result.getStatement()).thenReturn(boundStatement);
    when(result.getExecutionInfo()).thenReturn(Optional.ofNullable(executionInfo));
    when(executionInfo.getQueriedHost()).thenReturn(host);
    when(host.getSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
    ColumnDefinitions.Definition c1 = newDefinition("myKeyspace", "myTable", "c1", DataType.cint());
    ColumnDefinitions.Definition c2 =
        newDefinition("myKeyspace", "myTable", "c2", DataType.varchar());
    ColumnDefinitions.Definition c3 =
        newDefinition("myKeyspace", "myTable", "c3", DataType.varchar());
    ColumnDefinitions resultVariables = newColumnDefinitions(c1, c2, c3);
    when(row.getColumnDefinitions()).thenReturn(resultVariables);
    when(boundStatement.preparedStatement()).thenReturn(ps);
    when(ps.getQueryString()).thenReturn("irrelevant");
    // simulates a WHERE clause like token(...) > :start and token(...) <= :end and c1 = :c1
    ColumnDefinitions.Definition start =
        newDefinition("myKeyspace", "myTable", "start", DataType.bigint());
    ColumnDefinitions.Definition end =
        newDefinition("myKeyspace", "myTable", "end", DataType.bigint());
    ColumnDefinitions boundVariables = newColumnDefinitions(start, end, c1);
    when(ps.getVariables()).thenReturn(boundVariables);
    when(row.getObject("c1")).thenReturn(42);
    when(row.getObject("c2")).thenReturn("foo");
    when(row.getObject("c3")).thenReturn("bar");
    when(boundStatement.getObject("start")).thenReturn(1234L);
    when(boundStatement.getObject("end")).thenReturn(5678L);
    when(boundStatement.getObject("c1")).thenReturn(42);
  }

  @Test
  public void should_create_location_for_bound_statement() throws Exception {
    URI location = ResultUtils.getLocation(result);
    assertThat(location)
        .hasScheme("cql")
        .hasHost("127.0.0.1")
        .hasPort(9042)
        .hasPath("/myKeyspace/myTable")
        .hasParameter("start", "1234")
        .hasParameter("end", "5678")
        .hasParameter("c1", "42")
        .hasParameter("c2", "\'foo\'")
        .hasParameter("c3", "\'bar\'");
  }
}
