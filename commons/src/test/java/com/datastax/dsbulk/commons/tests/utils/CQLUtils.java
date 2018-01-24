/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

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
