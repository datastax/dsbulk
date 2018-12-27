/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.uri;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import java.net.InetSocketAddress;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URIUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(URIUtils.class);

  public static final URI UNKNOWN_ROW_RESOURCE = URI.create("cql://unknown");

  /**
   * Returns the resource {@link URI} of a row in a read result.
   *
   * <p>URIs returned by this method are of the following form:
   *
   * <pre>{@code
   * cql://host:port/keyspace/table
   * }</pre>
   *
   * @param row The row of the result
   * @param executionInfo The execution info of the result
   * @return The read result row resource URI.
   */
  public static URI getRowResource(Row row, ExecutionInfo executionInfo) {
    try {
      InetSocketAddress host = executionInfo.getQueriedHost().getSocketAddress();
      ColumnDefinitions resultVariables = row.getColumnDefinitions();
      // this might break if the statement has no result variables (unlikely)
      // or if the first variable is not associated to a keyspace and table (also unlikely)
      String keyspace = resultVariables.getKeyspace(0);
      String table = resultVariables.getTable(0);
      String sb =
          "cql://"
              + host.getAddress().getHostAddress()
              + ':'
              + host.getPort()
              + '/'
              + keyspace
              + '/'
              + table;
      return URI.create(sb);
    } catch (Exception e) {
      LOGGER.error("Cannot create URI for row", e);
      return UNKNOWN_ROW_RESOURCE;
    }
  }
}
