/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

public class DriverCoreCommonsTestHooks {

  public static PreparedId newPreparedId(
      ColumnDefinitions cd, int[] pkIndices, ProtocolVersion version) {
    return new PreparedId(
        new PreparedId.PreparedMetadata(null, null),
        new PreparedId.PreparedMetadata(null, cd),
        pkIndices,
        version);
  }

  public static ColumnDefinitions.Definition newDefinition(String name, DataType type) {
    return newDefinition("ks", "t", name, type);
  }

  public static ColumnDefinitions.Definition newDefinition(
      String keyspace, String table, String name, DataType type) {
    return new ColumnDefinitions.Definition(keyspace, table, name, type);
  }

  public static ColumnDefinitions newColumnDefinitions(ColumnDefinitions.Definition... cols) {
    return new ColumnDefinitions(cols, CodecRegistry.DEFAULT_INSTANCE);
  }

  public static TokenRange newTokenRange(Token start, Token end) {
    return new TokenRange(start, end, Token.M3PToken.FACTORY);
  }

  public static Token newToken(long value) {
    return Token.M3PToken.FACTORY.fromString(Long.toString(value));
  }
}
