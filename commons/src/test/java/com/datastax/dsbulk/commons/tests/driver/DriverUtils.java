/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver;

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DriverUtils {

  public static final Object UNSET = new Object();

  @NonNull
  public static CqlSession mockSession() {
    CqlSession session = mock(CqlSession.class);
    configureMockSession(session, ProtocolVersion.DEFAULT);
    return session;
  }

  @NonNull
  public static CqlSession mockCqlSession() {
    CqlSession session = mock(CqlSession.class);
    configureMockSession(session, DseProtocolVersion.DSE_V2);
    return session;
  }

  @NonNull
  public static Node mockNode() {
    return mockNode(UUID.randomUUID(), "127.0.0.1", "dc1");
  }

  @NonNull
  public static Node mockNode(UUID hostId, String address, String dataCenter) {
    Node h1 = mock(Node.class);
    when(h1.getCassandraVersion()).thenReturn(Version.parse("3.11.1"));
    when(h1.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("6.7.0")));
    when(h1.getEndPoint())
        .thenReturn(new DefaultEndPoint(InetSocketAddress.createUnresolved(address, 9042)));
    when(h1.getDatacenter()).thenReturn(dataCenter);
    when(h1.getHostId()).thenReturn(hostId);
    return h1;
  }

  private static void configureMockSession(CqlSession session, ProtocolVersion version) {
    DriverContext context = mock(DriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getContext()).thenReturn(context);
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(version);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(config.getProfile(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(profile);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
  }

  @SuppressWarnings("unchecked")
  public static BoundStatement mockBoundStatement(String query, Object... values) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(ps.getQuery()).thenReturn(query);
    BoundStatement bs = mock(BoundStatement.class);
    when(bs.size()).thenReturn(values.length);
    when(bs.getPreparedStatement()).thenReturn(ps);
    when(bs.codecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(bs.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    List<ColumnDefinition> defs = new ArrayList<>(values.length);
    if (values.length > 0) {
      List<ByteBuffer> bbs = new ArrayList<>(values.length);
      for (int i = 0; i < values.length; i++) {
        Object value = values[i];
        CqlIdentifier name = CqlIdentifier.fromInternal("c" + (i + 1));
        when(bs.isSet(i)).thenReturn(value != UNSET);
        when(bs.isSet(name)).thenReturn(value != UNSET);
        when(bs.isSet(name.asCql(true))).thenReturn(value != UNSET);
        when(bs.getObject(i)).thenReturn(value);
        when(bs.getObject(name)).thenReturn(value);
        when(bs.getObject(name.asCql(true))).thenReturn(value);
        if (value == null || value == UNSET) {
          bbs.add(null);
          defs.add(mockColumnDefinition(name, DataTypes.INT));
          when(bs.isNull(i)).thenReturn(value != UNSET);
          when(bs.isNull(name)).thenReturn(value != UNSET);
          when(bs.isNull(name.asCql(true))).thenReturn(value != UNSET);
          when(bs.getType(i)).thenReturn(DataTypes.INT);
          when(bs.getType(name)).thenReturn(DataTypes.INT);
          when(bs.getType(name.asCql(true))).thenReturn(DataTypes.INT);
          when(bs.get(i, TypeCodecs.INT)).thenReturn(null);
          when(bs.get(name, TypeCodecs.INT)).thenReturn(null);
          when(bs.get(name.asCql(true), TypeCodecs.INT)).thenReturn(null);
          when(bs.getBytesUnsafe(i)).thenReturn(null);
          when(bs.getBytesUnsafe(name)).thenReturn(null);
          when(bs.getBytesUnsafe(name.asCql(true))).thenReturn(null);
        } else {
          TypeCodec<Object> codec = CodecRegistry.DEFAULT.codecFor(value);
          ByteBuffer bb = codec.encode(value, V4);
          bbs.add(bb);
          defs.add(mockColumnDefinition(name, codec.getCqlType()));
          when(bs.isNull(i)).thenReturn(false);
          when(bs.isNull(name)).thenReturn(false);
          when(bs.isNull(name.asCql(true))).thenReturn(false);
          when(bs.getType(i)).thenReturn(codec.getCqlType());
          when(bs.getType(name)).thenReturn(codec.getCqlType());
          when(bs.getType(name.asCql(true))).thenReturn(codec.getCqlType());
          when(bs.get(i, codec)).thenReturn(value);
          when(bs.get(name, codec)).thenReturn(value);
          when(bs.get(name.asCql(true), codec)).thenReturn(value);
          when(bs.get(i, (Class<Object>) value.getClass())).thenReturn(value);
          when(bs.get(name, (Class<Object>) value.getClass())).thenReturn(value);
          when(bs.get(name.asCql(true), (Class<Object>) value.getClass())).thenReturn(value);
          when(bs.get(i, (GenericType<Object>) GenericType.of(value.getClass()))).thenReturn(value);
          when(bs.get(name, (GenericType<Object>) GenericType.of(value.getClass())))
              .thenReturn(value);
          when(bs.get(name.asCql(true), (GenericType<Object>) GenericType.of(value.getClass())))
              .thenReturn(value);
        }
      }
      when(bs.getValues()).thenReturn(bbs);
    }
    ColumnDefinitions definitions = mockColumnDefinitions(defs);
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    return bs;
  }

  @SuppressWarnings("unchecked")
  public static Row mockRow(Object... values) {
    Row row = mock(Row.class);
    when(row.size()).thenReturn(values.length);
    when(row.codecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(row.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    List<ColumnDefinition> defs = new ArrayList<>(values.length);
    if (values.length > 0) {
      for (int i = 0; i < values.length; i++) {
        Object value = values[i];
        CqlIdentifier name = CqlIdentifier.fromInternal("c" + (i + 1));
        when(row.getObject(i)).thenReturn(value);
        when(row.getObject(name)).thenReturn(value);
        when(row.getObject(name.asCql(true))).thenReturn(value);
        if (value == null) {
          defs.add(mockColumnDefinition(name, DataTypes.INT));
          when(row.isNull(i)).thenReturn(true);
          when(row.isNull(name)).thenReturn(true);
          when(row.isNull(name.asCql(true))).thenReturn(true);
          when(row.getType(i)).thenReturn(DataTypes.INT);
          when(row.getType(name)).thenReturn(DataTypes.INT);
          when(row.getType(name.asCql(true))).thenReturn(DataTypes.INT);
          when(row.get(i, TypeCodecs.INT)).thenReturn(null);
          when(row.get(name, TypeCodecs.INT)).thenReturn(null);
          when(row.get(name.asCql(true), TypeCodecs.INT)).thenReturn(null);
          when(row.getBytesUnsafe(i)).thenReturn(null);
          when(row.getBytesUnsafe(name)).thenReturn(null);
          when(row.getBytesUnsafe(name.asCql(true))).thenReturn(null);
        } else {
          TypeCodec<Object> codec = CodecRegistry.DEFAULT.codecFor(value);
          ByteBuffer bb = codec.encode(value, V4);
          defs.add(mockColumnDefinition(name, codec.getCqlType()));
          when(row.isNull(i)).thenReturn(false);
          when(row.isNull(name)).thenReturn(false);
          when(row.isNull(name.asCql(true))).thenReturn(false);
          when(row.getType(i)).thenReturn(codec.getCqlType());
          when(row.getType(name)).thenReturn(codec.getCqlType());
          when(row.getType(name.asCql(true))).thenReturn(codec.getCqlType());
          when(row.get(i, codec)).thenReturn(value);
          when(row.get(name, codec)).thenReturn(value);
          when(row.get(name.asCql(true), codec)).thenReturn(value);
          when(row.get(i, (Class<Object>) value.getClass())).thenReturn(value);
          when(row.get(name, (Class<Object>) value.getClass())).thenReturn(value);
          when(row.get(name.asCql(true), (Class<Object>) value.getClass())).thenReturn(value);
          when(row.get(i, (GenericType<Object>) GenericType.of(value.getClass())))
              .thenReturn(value);
          when(row.get(name, (GenericType<Object>) GenericType.of(value.getClass())))
              .thenReturn(value);
          when(row.get(name.asCql(true), (GenericType<Object>) GenericType.of(value.getClass())))
              .thenReturn(value);
          when(row.getBytesUnsafe(i)).thenReturn(bb.duplicate());
          when(row.getBytesUnsafe(name)).thenReturn(bb.duplicate());
          when(row.getBytesUnsafe(name.asCql(true))).thenReturn(bb.duplicate());
        }
      }
    }
    ColumnDefinitions definitions = mockColumnDefinitions(defs);
    when(row.getColumnDefinitions()).thenReturn(definitions);
    return row;
  }

  public static ColumnDefinition mockColumnDefinition(String name, DataType type) {
    return mockColumnDefinition(CqlIdentifier.fromInternal(name), type);
  }

  public static ColumnDefinition mockColumnDefinition(CqlIdentifier name, DataType type) {
    return mockColumnDefinition(
        CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("t"), name, type);
  }

  public static ColumnDefinition mockColumnDefinition(
      String keyspace, String table, String name, DataType type) {
    return mockColumnDefinition(
        CqlIdentifier.fromInternal(keyspace),
        CqlIdentifier.fromInternal(table),
        CqlIdentifier.fromInternal(name),
        type);
  }

  public static ColumnDefinition mockColumnDefinition(
      CqlIdentifier keyspace, CqlIdentifier table, CqlIdentifier name, DataType type) {
    ColumnDefinition def = mock(ColumnDefinition.class);
    when(def.isDetached()).thenReturn(false);
    when(def.getKeyspace()).thenReturn(keyspace);
    when(def.getTable()).thenReturn(table);
    when(def.getName()).thenReturn(name);
    when(def.getType()).thenReturn(type);
    return def;
  }

  public static ColumnDefinitions mockColumnDefinitions(ColumnDefinition... cols) {
    return mockColumnDefinitions(Arrays.asList(cols));
  }

  public static ColumnDefinitions mockColumnDefinitions(List<ColumnDefinition> cols) {
    return DefaultColumnDefinitions.valueOf(cols);
  }

  public static Murmur3TokenRange newTokenRange(Token start, Token end) {
    return new Murmur3TokenRange(((Murmur3Token) start), ((Murmur3Token) end));
  }

  public static Murmur3Token newToken(long value) {
    return new Murmur3Token(value);
  }

  public static TupleType mockTupleType(DataType... types) {
    return mockTupleType(ProtocolVersion.DEFAULT, CodecRegistry.DEFAULT, types);
  }

  public static TupleType mockTupleType(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types) {
    return new DefaultTupleType(
        Arrays.asList(types),
        new AttachmentPoint() {
          @Override
          @NonNull
          public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
          }

          @Override
          @NonNull
          public CodecRegistry getCodecRegistry() {
            return codecRegistry;
          }
        });
  }
}
