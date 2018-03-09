/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import java.util.Arrays;

@SuppressWarnings("SameParameterValue")
public class DriverCoreEngineTestHooks {

  public static PreparedId newPreparedId(ColumnDefinitions cd, ProtocolVersion version) {
    return new PreparedId(
        new PreparedId.PreparedMetadata(null, null),
        new PreparedId.PreparedMetadata(null, cd),
        null,
        version);
  }

  public static TupleType newTupleType(DataType... types) {
    return newTupleType(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, types);
  }

  public static TupleType newTupleType(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types) {
    return new TupleType(Arrays.asList(types), protocolVersion, codecRegistry);
  }

  public static UserType newUserType(UserType.Field... fields) {
    return newUserType(
        "ks", "udt", ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, fields);
  }

  public static UserType newUserType(CodecRegistry codecRegistry, UserType.Field... fields) {
    return newUserType("ks", "udt", ProtocolVersion.NEWEST_SUPPORTED, codecRegistry, fields);
  }

  private static UserType newUserType(
      String keyspace,
      String typeName,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      UserType.Field... fields) {
    return new UserType(
        keyspace, typeName, true, Arrays.asList(fields), protocolVersion, codecRegistry);
  }

  public static UserType.Field newField(String name, DataType type) {
    return new UserType.Field(name, type);
  }

  public static TokenRange newTokenRange(Token start, Token end) {
    return new TokenRange(start, end, Token.M3PToken.FACTORY);
  }

  public static Token newToken(long value) {
    return Token.M3PToken.FACTORY.fromString(Long.toString(value));
  }

  public static Statement wrappedStatement(StatementWrapper wrapper) {
    return wrapper.getWrappedStatement();
  }
}
