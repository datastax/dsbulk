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

  private static final String CREATE_KEYSPACE_SIMPLE_FORMAT =
      "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";

  public static String createKeyspaceSimpleStrategy(String keyspace, int replicationFactor) {
    return String.format(
        CREATE_KEYSPACE_SIMPLE_FORMAT, Metadata.quoteIfNecessary(keyspace), replicationFactor);
  }

  public static String createKeyspaceNetworkTopologyStrategy(
      String keyspace, int... replicationFactors) {
    StringBuilder sb =
        new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
            .append(Metadata.quoteIfNecessary(keyspace))
            .append(" WITH replication = { 'class' : 'NetworkTopologyStrategy', ");
    for (int i = 0; i < replicationFactors.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      int rf = replicationFactors[i];
      sb.append("'dc").append(i + 1).append("' : ").append(rf);
    }
    return sb.append('}').toString();
  }

  public static String truncateKeyspaceTable(String keyspace, String table) {
    return "TRUNCATE " + keyspace + "." + table;
  }
}
