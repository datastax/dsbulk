/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import com.datastax.driver.core.Metadata;

public abstract class CQLUtils {

  private static final String CREATE_KEYSPACE_FORMAT =
      "CREATE KEYSPACE %s WITH replication = { 'class' : '%s', 'replication_factor' : %d }";

  public static String createKeyspaceSimpleStrategy(String keyspace, int replicationFactor) {
    return createKeyspace(keyspace, "SimpleStrategy", replicationFactor);
  }

  private static String createKeyspace(String keyspace, String strategy, int replicationFactor) {
    return String.format(
        CREATE_KEYSPACE_FORMAT, Metadata.quoteIfNecessary(keyspace), strategy, replicationFactor);
  }
}
