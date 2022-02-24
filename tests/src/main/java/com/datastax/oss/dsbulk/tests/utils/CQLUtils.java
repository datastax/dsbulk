/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;

public abstract class CQLUtils {

  private static final String CREATE_KEYSPACE_SIMPLE_FORMAT =
      "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";

  public static SimpleStatement createKeyspaceSimpleStrategy(
      String keyspace, int replicationFactor) {
    return createKeyspaceSimpleStrategy(CqlIdentifier.fromInternal(keyspace), replicationFactor);
  }

  public static SimpleStatement createKeyspaceSimpleStrategy(
      CqlIdentifier keyspace, int replicationFactor) {
    return SimpleStatement.newInstance(
        String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace.asCql(true), replicationFactor));
  }

  public static SimpleStatement createKeyspaceNetworkTopologyStrategy(
      String keyspace, int... replicationFactors) {
    return createKeyspaceNetworkTopologyStrategy(
        CqlIdentifier.fromInternal(keyspace), replicationFactors);
  }

  public static SimpleStatement createKeyspaceNetworkTopologyStrategy(
      CqlIdentifier keyspace, int... replicationFactors) {
    StringBuilder sb =
        new StringBuilder("CREATE KEYSPACE ")
            .append(keyspace.asCql(true))
            .append(" WITH replication = { 'class' : 'NetworkTopologyStrategy', ");
    for (int i = 0; i < replicationFactors.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      int rf = replicationFactors[i];
      sb.append("'dc").append(i + 1).append("' : ").append(rf);
    }
    return SimpleStatement.newInstance(sb.append('}').toString());
  }

  public static SimpleStatement truncateTable(String keyspace, String table) {
    return truncateTable(CqlIdentifier.fromInternal(keyspace), CqlIdentifier.fromInternal(table));
  }

  public static SimpleStatement truncateTable(CqlIdentifier keyspace, CqlIdentifier table) {
    return SimpleStatement.newInstance(
        "TRUNCATE " + keyspace.asCql(true) + "." + table.asCql(true));
  }

  public static boolean isCqlTypeSupported(
      DataType cqlType, ProtocolVersion version, Version cassandraVersion) {
    switch (cqlType.getProtocolCode()) {
      case ProtocolConstants.DataType.DATE:
      case ProtocolConstants.DataType.TIME:
      case ProtocolConstants.DataType.SMALLINT:
      case ProtocolConstants.DataType.TINYINT:
        return version.getCode() >= 4;
      case ProtocolConstants.DataType.DURATION:
        return version.getCode() >= 5;
      case ProtocolConstants.DataType.UDT:
        // https://issues.apache.org/jira/browse/CASSANDRA-7423
        return ((UserDefinedType) cqlType).isFrozen()
            || cassandraVersion.compareTo(Version.parse("3.6")) >= 0;
    }
    return true;
  }
}
