/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import java.util.Arrays;

@SuppressWarnings("SameParameterValue")
public class DriverCoreEngineTestHooks {
  private static final CodecRegistry CODEC_REGISTRY = new DefaultCodecRegistry("test");

  // TODO: Update to work with next-gen Java driver.
  //  public static PreparedId newPreparedId(
  //      ColumnDefinitions cd, int[] pkIndices, ProtocolVersion version) {
  //    return new PreparedId(
  //        new PreparedId.PreparedMetadata(null, null),
  //        new PreparedId.PreparedMetadata(null, cd),
  //        pkIndices,
  //        version);
  //  }

  public static TupleType newTupleType(DataType... types) {
    return newTupleType(DseProtocolVersion.DSE_V2, CODEC_REGISTRY, types);
  }

  public static TupleType newTupleType(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types) {
    return new DefaultTupleType(
        Arrays.asList(types),
        new AttachmentPoint() {
          @Override
          public ProtocolVersion protocolVersion() {
            return protocolVersion;
          }

          @Override
          public CodecRegistry codecRegistry() {
            return codecRegistry;
          }
        });
  }

  //  public static UserDefinedType newUserType(String[] fieldNames, String[] fieldTypes) {
  //    return newUserType(
  //        "ks", "udt", DseProtocolVersion.DSE_V2, CODEC_REGISTRY, fields);
  //  }
  //
  //  public static UserDefinedType newUserType(CodecRegistry codecRegistry, String[] fieldNames,
  // String[] fieldTypes) {
  //    return newUserType("ks", "udt", ProtocolVersion.NEWEST_SUPPORTED, codecRegistry, fields);
  //  }
  //
  //  private static UserDefinedType newUserType(
  //      String keyspace,
  //      String typeName,
  //      ProtocolVersion protocolVersion,
  //      CodecRegistry codecRegistry,
  //      String[] fieldNames,
  //      String[] fieldTypes) {
  //    UserDefinedTypeBuilder
  //    return new UserDefinedType()(
  //        keyspace, typeName, true, Arrays.asList(fields), protocolVersion, codecRegistry);
  //  }

  public static TokenRange newTokenRange(Token start, Token end) {
    return new Murmur3TokenRange((Murmur3Token) start, (Murmur3Token) end);
  }

  public static Token newToken(long value) {
    return new Murmur3TokenFactory().parse(Long.toString(value));
  }

  // TODO: Update to work with next-gen Java driver.
  //  public static ByteBuffer compose(ByteBuffer... bbs) {
  //    return SimpleStatement.compose(bbs);
  //  }
}
