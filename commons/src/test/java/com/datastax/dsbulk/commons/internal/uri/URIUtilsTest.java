/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.uri;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import java.net.InetSocketAddress;
import java.net.URI;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class URIUtilsTest {
  private BoundStatement boundStatement;
  private Row row;
  private ExecutionInfo executionInfo;

  @BeforeEach
  void setUp() {
    row = mock(Row.class);
    executionInfo = mock(ExecutionInfo.class);
    boundStatement = mock(BoundStatement.class);
    when(row.codecRegistry()).thenReturn(new DefaultCodecRegistry("test"));
    Node node = mock(Node.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(executionInfo.getCoordinator()).thenReturn(node);
    when(node.getConnectAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));

    ColumnDefinition c1 =
        new DefaultColumnDefinition(
            new ColumnSpec(
                "myKeyspace",
                "myTable",
                "c1",
                0,
                RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT)),
            AttachmentPoint.NONE);
    ColumnDefinition c2 =
        new DefaultColumnDefinition(
            new ColumnSpec(
                "myKeyspace",
                "myTable",
                "c2",
                1,
                RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)),
            AttachmentPoint.NONE);
    ColumnDefinition c3 =
        new DefaultColumnDefinition(
            new ColumnSpec(
                "myKeyspace",
                "myTable",
                "c3",
                2,
                RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)),
            AttachmentPoint.NONE);
    ColumnDefinitions resultVariables =
        DefaultColumnDefinitions.valueOf(Lists.newArrayList(c1, c2, c3));
    when(row.getColumnDefinitions()).thenReturn(resultVariables);

    when(boundStatement.getPreparedStatement()).thenReturn(ps);
    when(ps.getQuery()).thenReturn("irrelevant");
    // simulates a WHERE clause like token(...) > :start and token(...) <= :end and c1 = :c1
    ColumnDefinition start =
        new DefaultColumnDefinition(
            new ColumnSpec(
                "myKeyspace",
                "myTable",
                "start",
                0,
                RawType.PRIMITIVES.get(ProtocolConstants.DataType.BIGINT)),
            AttachmentPoint.NONE);
    ColumnDefinition end =
        new DefaultColumnDefinition(
            new ColumnSpec(
                "myKeyspace",
                "myTable",
                "end",
                1,
                RawType.PRIMITIVES.get(ProtocolConstants.DataType.BIGINT)),
            AttachmentPoint.NONE);
    ColumnDefinitions boundVariables =
        DefaultColumnDefinitions.valueOf(Lists.newArrayList(start, end, c1));
    when(ps.getVariableDefinitions()).thenReturn(boundVariables);
    when(row.getObject(CqlIdentifier.fromCql("c1"))).thenReturn(42);
    when(row.getObject(CqlIdentifier.fromCql("c2"))).thenReturn("foo");
    when(row.getObject(CqlIdentifier.fromCql("c3"))).thenReturn("bar");
    when(boundStatement.getObject(CqlIdentifier.fromCql("start"))).thenReturn(1234L);
    when(boundStatement.getObject(CqlIdentifier.fromCql("end"))).thenReturn(5678L);
    when(boundStatement.getObject(CqlIdentifier.fromCql("c1"))).thenReturn(42);
  }

  @Test
  void should_create_location_for_bound_statement() {
    URI location = URIUtils.getRowLocation(row, executionInfo, boundStatement);
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
