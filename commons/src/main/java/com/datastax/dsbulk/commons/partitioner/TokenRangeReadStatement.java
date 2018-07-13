/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;

public class TokenRangeReadStatement extends StatementWrapper {

  private TokenRange<?, ?> range;
  private final com.datastax.driver.core.Token routingToken;
  private final String keyspaceName;
  private final String tableName;

  public TokenRangeReadStatement(
      Statement wrapped, TableMetadata table, TokenRange<?, ?> range, Token routingToken) {
    super(wrapped);
    this.range = range;
    this.routingToken = routingToken;
    // DO NOT quote CQL identifiers here, the TAP policy requires unquoted keyspace names
    this.keyspaceName = table.getKeyspace().getName();
    this.tableName = table.getName();
  }

  @Override
  public com.datastax.driver.core.Token getRoutingToken() {
    return routingToken;
  }

  public TokenRange<?, ?> geTokenRange() {
    return range;
  }

  @Override
  public String getKeyspace() {
    return keyspaceName;
  }

  @Override
  public String toString() {
    return String.format(
        "TokenRangeReadStatement{table=%s.%s,range=%s}",
        Metadata.quoteIfNecessary(keyspaceName), Metadata.quoteIfNecessary(tableName), range);
  }
}
