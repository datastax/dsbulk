/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.uri;

import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Row;
import java.net.InetSocketAddress;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class URIUtilsTest {

  private Row row;
  private ExecutionInfo executionInfo;

  @BeforeEach
  void setUp() {
    row = mock(Row.class);
    executionInfo = mock(ExecutionInfo.class);
    Host host = mock(Host.class);
    when(executionInfo.getQueriedHost()).thenReturn(host);
    when(host.getSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
    Definition c1 = newDefinition("myKeyspace", "myTable", "c1", DataType.cint());
    ColumnDefinitions resultVariables = newColumnDefinitions(c1);
    when(row.getColumnDefinitions()).thenReturn(resultVariables);
  }

  @Test
  void should_create_location_for_row() {
    URI location = URIUtils.getRowResource(row, executionInfo);
    assertThat(location)
        .hasScheme("cql")
        .hasHost("127.0.0.1")
        .hasPort(9042)
        .hasPath("/myKeyspace/myTable")
        .hasNoParameters();
  }
}
