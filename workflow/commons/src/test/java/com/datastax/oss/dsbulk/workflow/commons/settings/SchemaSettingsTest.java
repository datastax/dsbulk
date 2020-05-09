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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V3;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.COUNTER;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static com.datastax.oss.dsbulk.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.INTERNAL;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockColumnDefinition;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockColumnDefinitions;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockSession;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newToken;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newTokenRange;
import static com.datastax.oss.dsbulk.tests.utils.ReflectionUtils.getInternalState;
import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.ranges;
import static java.time.Instant.EPOCH;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.WARN;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphKeyspaceMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.SetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.DefaultMapping;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.typesafe.config.Config;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

@ExtendWith(LogInterceptingExtension.class)
class SchemaSettingsTest {

  private static final String NULL_TO_UNSET = "nullToUnset";

  private static final CqlIdentifier C1 = CqlIdentifier.fromInternal("c1");
  private static final CqlIdentifier C2 =
      CqlIdentifier.fromInternal("This is column 2, and its name desperately needs quoting");
  private static final CqlIdentifier C3 = CqlIdentifier.fromInternal("c3");
  private static final CqlIdentifier C4 = CqlIdentifier.fromInternal("c4");

  private final Murmur3Token token1 = newToken(-9223372036854775808L);
  private final Murmur3Token token2 = newToken(-3074457345618258603L);
  private final Murmur3Token token3 = newToken(3074457345618258602L);

  private final Set<TokenRange> tokenRanges =
      Sets.newHashSet(
          newTokenRange(token1, token2),
          newTokenRange(token2, token3),
          newTokenRange(token3, token1));

  private final ConvertingCodecFactory codecFactory = mock(ConvertingCodecFactory.class);
  private final RecordMetadata recordMetadata = (field, cqlType) -> GenericType.STRING;

  private final LogInterceptor logs;

  private CqlSession session;
  private DriverContext context;
  private Metadata metadata;
  private DseGraphKeyspaceMetadata keyspace;
  private DseGraphTableMetadata table;
  private PreparedStatement ps;
  private ColumnMetadata col1;
  private ColumnMetadata col2;
  private ColumnMetadata col3;

  SchemaSettingsTest(@LogCapture(level = WARN) LogInterceptor logs) {
    this.logs = logs;
  }

  @SuppressWarnings("unused")
  private static List<ProtocolVersion> allProtocolVersions() {
    List<ProtocolVersion> versions = Lists.newArrayList(DefaultProtocolVersion.values());
    versions.addAll(Arrays.asList(DseProtocolVersion.values()));
    return versions;
  }

  @BeforeEach
  void setUp() {
    session = mockSession();
    context = mock(DriverContext.class);
    when(session.getContext()).thenReturn(context);
    when(context.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    metadata = mock(Metadata.class);
    DefaultTokenMap tokenMap = mock(DefaultTokenMap.class);
    keyspace = mock(DseGraphKeyspaceMetadata.class);
    table = mock(DseGraphTableMetadata.class);
    ps = mock(PreparedStatement.class);
    col1 = mock(ColumnMetadata.class);
    col2 = mock(ColumnMetadata.class);
    col3 = mock(ColumnMetadata.class);
    when(session.getMetadata()).thenReturn(metadata);
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal("ks");
    CqlIdentifier tableId = CqlIdentifier.fromInternal("t1");
    CqlIdentifier viewId = CqlIdentifier.fromInternal("mv1");
    when(metadata.getKeyspace(keyspaceId)).thenReturn(Optional.of(keyspace));
    when(metadata.getKeyspaces()).thenReturn(ImmutableMap.of(keyspaceId, keyspace));
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getTokenRanges()).thenReturn(tokenRanges);
    when(tokenMap.parse(String.valueOf(token1.getValue()))).thenReturn(token1);
    when(tokenMap.parse(String.valueOf(token2.getValue()))).thenReturn(token2);
    when(tokenMap.parse(String.valueOf(token3.getValue()))).thenReturn(token3);
    when(tokenMap.getPartitionerName()).thenReturn("Murmur3Partitioner");
    when(tokenMap.getReplicas(anyString(), any(Token.class))).thenReturn(Collections.emptySet());
    when(tokenMap.getTokenFactory()).thenReturn(new Murmur3TokenFactory());
    when(keyspace.getTable(tableId)).thenReturn(Optional.of(table));
    when(keyspace.getTables()).thenReturn(ImmutableMap.of(tableId, table));
    ViewMetadata materializedView = mock(ViewMetadata.class);
    when(materializedView.getName()).thenReturn(viewId);
    when(keyspace.getView(viewId)).thenReturn(Optional.of(materializedView));
    when(keyspace.getViews()).thenReturn(ImmutableMap.of(viewId, materializedView));
    when(keyspace.getName()).thenReturn(keyspaceId);
    when(session.prepare(anyString())).thenReturn(ps);
    Map<CqlIdentifier, ColumnMetadata> columns = ImmutableMap.of(C1, col1, C2, col2, C3, col3);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1)).thenReturn(Optional.of(col1));
    when(table.getColumn(C2)).thenReturn(Optional.of(col2));
    when(table.getColumn(C3)).thenReturn(Optional.of(col3));
    when(table.getPrimaryKey()).thenReturn(Collections.singletonList(col1));
    when(table.getPartitionKey()).thenReturn(Collections.singletonList(col1));
    when(table.getKeyspace()).thenReturn(keyspaceId);
    when(table.getName()).thenReturn(tableId);
    when(col1.getName()).thenReturn(C1);
    when(col2.getName()).thenReturn(C2);
    when(col3.getName()).thenReturn(C3);
    when(col1.getType()).thenReturn(TEXT);
    when(col2.getType()).thenReturn(TEXT);
    when(col3.getType()).thenReturn(TEXT);
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition(C1, TEXT),
            mockColumnDefinition(C2, TEXT),
            mockColumnDefinition(C3, TEXT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    when(ps.getResultSetDefinitions()).thenReturn(definitions);
    when(ps.getId()).thenReturn(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_keyspace_and_table_provided(
      ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_keyspace_and_counter_table_provided(
      ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(col1.getType()).thenReturn(COUNTER);
    when(col2.getType()).thenReturn(COUNTER);
    when(col3.getType()).thenReturn(COUNTER);
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "UPDATE ks.t1 SET \"%2$s\" = \"%2$s\" + :\"%2$s\", %3$s = %3$s + :%3$s WHERE %1$s = :%1$s",
                C1, C2, C3));
    DefaultMapping mapping = (DefaultMapping) getInternalState(recordMapper, "mapping");
    assertMapping(mapping, C2, C2, C1, C1, C3, C3);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @Test
  void should_fail_to_create_schema_settings_when_mapping_many_to_one() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "mapping", "\" 0 = f1, 1 = f1\"", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid schema.mapping: the following variables are mapped to more than one field: f1. Please review schema.mapping for duplicates.");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_ttl_and_timestamp(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s, 1=__ttl, 3=__timestamp \", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s) USING TTL :\"[ttl]\" AND TIMESTAMP :\"[timestamp]\"",
                C1, C2));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1,
        "1",
        INTERNAL_TTL_VARNAME.render(INTERNAL),
        "3",
        INTERNAL_TIMESTAMP_VARNAME.render(INTERNAL));
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_function(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" now() = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (now(), :%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "2", C1);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_static_ttl(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "keyspace",
            "ks",
            "table",
            "t1",
            "queryTtl",
            30);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s) USING TTL 30",
                C1, C2));
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_static_timestamp(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "keyspace",
            "ks",
            "table",
            "t1",
            "queryTimestamp",
            "\"2017-01-02T00:00:01Z\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s) USING TIMESTAMP %3$s",
                C1,
                C2,
                instantToNumber(Instant.parse("2017-01-02T00:00:01Z"), MICROSECONDS, EPOCH)));
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_static_timestamp_and_ttl(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "keyspace",
            "ks",
            "table",
            "t1",
            "queryTimestamp",
            "\"2017-01-02T00:00:01Z\"",
            "queryTtl",
            25);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s) "
                    + "USING TTL 25 AND TIMESTAMP %3$s",
                C1,
                C2,
                instantToNumber(Instant.parse("2017-01-02T00:00:01Z"), MICROSECONDS, EPOCH)));
  }

  @Test
  void should_create_record_mapper_when_using_custom_query() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("c1var", DataTypes.TEXT),
            mockColumnDefinition("c2var", DataTypes.TEXT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    when(table.getColumn(CqlIdentifier.fromInternal("c2"))).thenReturn(Optional.of(col2));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"0 = c1var , 2 = c2var\"",
            "query",
            "\"INSERT INTO ks.t1 (c2, c1) VALUES (:c2var, :c1var)\"",
            "nullToUnset",
            true);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1 (c2, c1) VALUES (:c2var, :c1var)");
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), "0", "c1var", "2", "c2var");
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_is_a_list_and_indexed(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\"\\\"%2$s\\\", %1$s\", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "1", C1);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_is_a_list_and_mapped(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\"\\\"%2$s\\\", %1$s\", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1 (\"%2$s\", %1$s) VALUES (:\"%2$s\", :%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @Test
  void should_create_record_mapper_when_mapping_and_statement_provided() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "query",
            String.format(
                "\"insert into ks.t1 (%1$s,\\\"%2$s\\\") values (:%1$s, :\\\"%2$s\\\")\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.t1 (%1$s,\"%2$s\") values (:%1$s, :\"%2$s\")", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_keyspace_and_table_provided(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (%1$s, \"%2$s\", %3$s) VALUES (:%1$s, :\"%2$s\", :%3$s)",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2, C3, C3);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_inferred_mapping_and_override(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but override to set c4 source field to C3 column.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (%3$s, %1$s, \"%2$s\") VALUES (:%3$s, :%1$s, :\"%2$s\")",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2, C4, C3);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_inferred_mapping_and_skip(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but skip C2.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1 (%1$s, %2$s) VALUES (:%1$s, :%2$s)", C1, C3));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C3, C3);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_with_inferred_mapping_and_skip_multiple(
      ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but skip C2 and C3.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1 (%1$s) VALUES (:%1$s)", C1));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1);
    if (version.getCode() < V4.getCode()) {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    } else {
      assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_null_to_unset_is_false(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "nullToUnset", false, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (%1$s, \"%2$s\", %3$s) VALUES (:%1$s, :\"%2$s\", :%3$s)",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2, C3, C3);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_mapping_keyspace_and_table_provided(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\", %1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_mapping_is_a_list_and_indexed(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\"\\\"%2$s\\\", %1$s\", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\", %1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "1", C1);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_mapping_is_a_list_and_mapped(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\"\\\"%2$s\\\", %1$s\", ", C1, C2),
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\", %1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_with_inferred_mapping_and_override(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but override to set c4 source field to C3 column.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %3$s, %1$s, \"%2$s\" FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2, C4, C3);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_with_inferred_mapping_and_skip(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but skip C2.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s, %2$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C3));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C3, C3);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_with_inferred_mapping_and_skip_multiple(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    // Infer mapping, but skip C2 and C3.
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "nullToUnset",
            true,
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            String.format("\" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end", C1));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_mapping_and_statement_provided(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "query",
            String.format("\"select \\\"%2$s\\\", %1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "select \"%2$s\", %1$s from ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_keyspace_and_table_provided(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s, \"%2$s\", %3$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));

    assertMapping(
        (DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2, C3, C3);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_mapper_when_null_to_unset_is_false(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "nullToUnset", false, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s, \"%2$s\", %3$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2, C3, C3);
  }

  @Test
  void should_use_default_writetime_var_name() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" *=*, f1 = __timestamp \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    @SuppressWarnings("unchecked")
    Set<CQLWord> writeTimeVariables =
        (Set<CQLWord>) getInternalState(mapping, "writeTimeVariables");
    assertThat(writeTimeVariables).containsOnly(INTERNAL_TIMESTAMP_VARNAME);
  }

  @Test
  void should_detect_writetime_var_in_query() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("c1", DataTypes.TEXT),
            mockColumnDefinition("c2", DataTypes.TEXT),
            mockColumnDefinition("c3", DataTypes.TEXT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    when(table.getColumn(CqlIdentifier.fromInternal("c2"))).thenReturn(Optional.of(col2));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1,c2) VALUES (:c1, :c2) USING TIMESTAMP :c3\"",
            "mapping",
            "\" f1 = c1 , f2 = c2 , f3 = c3 \" ");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    @SuppressWarnings("unchecked")
    Set<CQLWord> writeTimeVariables =
        (Set<CQLWord>) getInternalState(mapping, "writeTimeVariables");
    assertThat(writeTimeVariables).containsOnly(CQLWord.fromInternal(C3.asInternal()));
  }

  @Test
  void should_detect_quoted_writetime_var_in_query() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("c1", DataTypes.TEXT),
            mockColumnDefinition("c2", DataTypes.TEXT),
            mockColumnDefinition("\"This is a quoted \\\" variable name\"", DataTypes.TEXT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    when(table.getColumn(CqlIdentifier.fromInternal("c2"))).thenReturn(Optional.of(col2));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1,c2) VALUES (:c1, :c2) USING TTL 123 AND tImEsTaMp     :\\\"This is a quoted \\\"\\\" variable name\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    @SuppressWarnings("unchecked")
    Set<CQLWord> writeTimeVariables =
        (Set<CQLWord>) getInternalState(mapping, "writeTimeVariables");
    assertThat(writeTimeVariables)
        .containsOnly(CQLWord.fromInternal("This is a quoted \" variable name"));
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_include_function_call_in_insert_statement(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo("INSERT INTO ks.t1 (c1, c3) VALUES (:c1, now())");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_include_function_call_in_select_statement(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo(
            "SELECT c1, now() AS \"now()\" FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_error_when_misplaced_function_call_in_insert_statement() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Misplaced function call detected on the right side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_error_when_misplaced_function_call_in_select_statement() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Misplaced function call detected on the left side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_create_single_read_statement_when_no_variables() {
    when(ps.getVariableDefinitions()).thenReturn(mockColumnDefinitions());
    BoundStatement bs = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs);
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "query", "\"SELECT a,b,c FROM ks.t1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<?> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(1);
    assertThat(statements.get(0)).isEqualTo(bs);
  }

  @Test
  void should_create_multiple_read_statements() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<Statement<?>> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(3).contains(bs1, bs2, bs3);
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) > :start and token(a) <= :end \"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<Statement<?>> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(3).contains(bs1, bs2, bs3);
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query_positional() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("partition key token", BIGINT),
            mockColumnDefinition("partition key token", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(1, token1)).thenReturn(bs1);
    when(bs1.setToken(0, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(1, token2)).thenReturn(bs2);
    when(bs2.setToken(0, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(1, token3)).thenReturn(bs3);
    when(bs3.setToken(0, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) <= ? AND token(a) > ?\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<Statement<?>> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(3).contains(bs1, bs2, bs3);
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query_for_counting() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<Statement<?>> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(3).contains(bs1, bs2, bs3);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_global_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecFactory, EnumSet.of(global), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_partition_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(col2, ClusteringOrder.ASC));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecFactory, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_hosts_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecFactory, EnumSet.of(hosts), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT token(c1) FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_ranges_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecFactory, EnumSet.of(ranges), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT token(c1) FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_partitions_and_ranges_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(col2, ClusteringOrder.ASC));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(
            session, codecFactory, EnumSet.of(partitions, ranges), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_use_custom_query_when_mode_is_global() {
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(col2, ClusteringOrder.ASC));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecFactory, EnumSet.of(global), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT c1, c3 FROM ks.t1 WHERE c1 = 0");
  }

  @Test
  void should_throw_when_custom_query_and_mode_is_not_global() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    assertThatThrownBy(
            () ->
                schemaSettings.createReadResultCounter(
                    session, codecFactory, EnumSet.of(hosts, ranges, partitions), 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot count with stats.modes = [ranges, hosts, partitions] when schema.query is provided; only stats.modes = [global] is allowed");
  }

  @Test
  void should_detect_named_variables_in_token_range_restriction() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("My Start", BIGINT), mockColumnDefinition("My End", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs1);
    when(bs1.setToken("\"My Start\"", token1)).thenReturn(bs1);
    when(bs1.setToken("\"My End\"", token2)).thenReturn(bs1);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) > :\\\"My Start\\\" and token(a) <= :\\\"My End\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM t1 WHERE token(a) > :\"My Start\" and token(a) <= :\"My End\"");
  }

  @Test
  void should_throw_configuration_exception_when_read_statement_variables_not_recognized() {
    ColumnDefinitions definitions = mockColumnDefinitions(mockColumnDefinition("bar", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE foo = :bar\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(session))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
                + "the WHERE clause is only allowed to contain one token range restriction of the form: "
                + "WHERE token(...) > ? AND token(...) <= ?");
  }

  @Test
  void should_throw_configuration_exception_when_read_statement_variables_not_recognized2() {
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("foo", BIGINT), mockColumnDefinition("bar", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) >= :foo and token(a) < :bar \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(session))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
                + "the WHERE clause is only allowed to contain one token range restriction of the form: "
                + "WHERE token(...) > ? AND token(...) <= ?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found_but_similar_ks_exists() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "KS", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Keyspace \"KS\" does not exist, however a keyspace ks was found. Did you mean to use -k ks?");
  }

  @Test
  void should_warn_that_table_was_not_found_but_similar_table_exists() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "T1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Table \"T1\" does not exist, however a table t1 was found. Did you mean to use -t t1?");
  }

  @Test
  void should_warn_that_table_was_not_found_but_similar_mv_exists() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MV1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Table or materialized view \"MV1\" does not exist, however a materialized view mv1 was found. Did you mean to use -t mv1?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "MyKs", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Keyspace \"MyKs\" does not exist");
  }

  @Test
  void should_warn_that_table_was_not_found() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mv_was_not_found() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table or materialized view \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mapped_fields_not_supported() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"c1=c1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
  }

  @Test
  void should_error_invalid_schema_settings() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then either schema.keyspace or schema.graph must be defined, and either schema.table, schema.vertex or schema.edge must be defined");
  }

  @Test
  void should_error_invalid_schema_mapping_missing_keyspace_and_table() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "mapping", "\"c1=c2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then either schema.keyspace or schema.graph must be defined, and either schema.table, schema.vertex or schema.edge must be defined");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_provided() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"", "keyspace", "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_not_qualified_and_keyspace_not_provided() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_non_existent() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("ks"))).thenReturn(Optional.empty());
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema.query references a non-existent keyspace: ks");
  }

  @Test
  void should_error_when_query_is_provided_and_table_non_existent() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("t1"))).thenReturn(Optional.empty());
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.query references a non-existent table or materialized view: t1");
  }

  @Test
  void should_error_invalid_schema_query_with_ttl() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "queryTtl",
            30,
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_timestamp() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "queryTimestamp",
            "\"2018-05-18T15:00:00Z\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_timestamp() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "mapping",
            "\"f1=__timestamp\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "__timestamp variable is not allowed when schema.query does not contain a USING TIMESTAMP clause");
  }

  @Test
  void should_error_invalid_schema_query_with_keyspace_and_table() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "keyspace",
            "keyspace",
            "table",
            "table");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.table, schema.vertex or schema.edge are defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_ttl() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "mapping",
            "\"f1=__ttl\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "__ttl variable is not allowed when schema.query does not contain a USING TTL clause");
  }

  @Test
  void should_error_when_mapping_provided_and_count_workflow() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"col1,col2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema.mapping must not be defined when counting rows in a table");
  }

  @Test
  void should_error_invalid_timestamp() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "queryTimestamp", "junk", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expecting schema.queryTimestamp to be in ISO_ZONED_DATE_TIME format but got 'junk'");
  }

  @Test
  void should_error_invalid_schema_missing_keyspace() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "schema.keyspace or schema.graph must be defined if schema.table, schema.vertex or schema.edge are defined");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present_load() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"now() = c1, 0 = c2\"",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Setting schema.query must not be defined when loading "
                + "if schema.mapping contains a function on the left side of a mapping entry");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present_unload() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"f1 = now(), f2 = c2\"",
            "query",
            "\"SELECT c1, c2 FROM t1\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Setting schema.query must not be defined when unloading "
                + "if schema.mapping contains a function on the right side of a mapping entry");
  }

  @Test
  void should_throw_exception_when_nullToUnset_not_a_boolean() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "nullToUnset", "NotABoolean");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.schema.nullToUnset, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_column() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecFactory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Schema mapping entry \"nonExistentCol\" doesn't match any column found in table t1");
  }

  @Test
  void should_error_when_graph_options_provided_but_cluster_is_not_compatible() {
    DseVertexMetadata vertexMetadata = mock(DseVertexMetadata.class);
    when(table.getVertex()).thenAnswer(x -> Optional.of(vertexMetadata));
    when(vertexMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("v1"));
    Node node = mock(Node.class);
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(UUID.randomUUID(), node));

    Map<String, Object> extras =
        ImmutableMap.of(DseNodeProperties.DSE_VERSION, Version.parse("6.0.0"));
    when(node.getExtras()).thenReturn(extras);
    when(node.toString()).thenReturn("host1");
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Graph operations not available due to incompatible cluster");
    assertThat(logs)
        .hasMessageContaining(
            "Incompatible cluster detected. Graph functionality is only compatible with")
        .hasMessageContaining("The following nodes do not appear to be running DSE")
        .hasMessageContaining("host1");
  }

  @Test
  void should_error_when_graph_options_provided_but_keyspace_not_graph() {
    DseVertexMetadata vertexMetadata = mock(DseVertexMetadata.class);
    when(table.getVertex()).thenAnswer(x -> Optional.of(vertexMetadata));
    when(vertexMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("v1"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Graph operations requested but provided keyspace is not a graph: ks");
  }

  @Test
  void should_error_when_graph_options_provided_but_keyspace_not_core_graph() {
    DseVertexMetadata vertexMetadata = mock(DseVertexMetadata.class);
    when(table.getVertex()).thenAnswer(x -> Optional.of(vertexMetadata));
    when(vertexMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("v1"));
    when(keyspace.getGraphEngine()).thenReturn(Optional.of("Classic"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Graph operations requested but provided graph ks was created with an unsupported graph engine: Classic");
  }

  @Test
  void should_warn_when_keyspace_is_core_graph_but_non_graph_options_provided() {
    when(keyspace.getGraphEngine()).thenReturn(Optional.of("Core"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    assertThat(logs)
        .hasMessageContaining(
            "Provided keyspace is a graph; "
                + "instead of schema.keyspace and schema.table, please use graph-specific options "
                + "such as schema.graph, schema.vertex, schema.edge, schema.from and schema.to.");
  }

  @Test
  void should_warn_when_keyspace_is_classic_graph_and_workflow_is_load() {
    when(keyspace.getGraphEngine()).thenReturn(Optional.of("Classic"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    assertThat(logs)
        .hasMessageContaining(
            "Provided keyspace is a graph created with a legacy graph engine: "
                + "Classic; attempting to load data into such a keyspace is not supported and "
                + "may put the graph in an inconsistent state.");
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_bound_variable() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)\"",
            "mapping",
            "\"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecFactory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Schema mapping entry \"nonExistentCol\" doesn't match any bound variable found in query: 'INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)'");
  }

  @Test
  void should_error_when_mapping_does_not_contain_primary_key() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"fieldA = c3\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecFactory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void should_error_when_insert_query_does_not_contain_primary_key() {
    when(table.getColumn(CqlIdentifier.fromInternal("c2"))).thenReturn(Optional.of(col2));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (c2) VALUES (:c2)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecFactory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void
      should_not_error_when_insert_query_does_not_contain_clustering_column_but_mutation_is_static_only() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (c1, c3) VALUES (:c1, :c3)\"");
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2));
    when(table.getPartitionKey()).thenReturn(singletonList(col1));
    when(col3.isStatic()).thenReturn(true);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
  }

  @Test
  void should_error_when_counting_partitions_but_table_has_no_clustering_column() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, true);
    assertThatThrownBy(
            () ->
                schemaSettings.createReadResultCounter(
                    session, codecFactory, EnumSet.of(partitions), 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot count partitions for table t1: it has no clustering column.");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_insert_where_clause_in_select_statement_simple(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "query", "\"SELECT a,b,c FROM t1\"", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<?> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT a,b,c FROM t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_insert_where_clause_in_select_statement_complex(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 ALLOW FILTERING\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<?> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM t1 WHERE token(c1) > :start AND token(c1) <= :end ALLOW FILTERING");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_insert_where_clause_in_select_statement_case_sensitive(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(Optional.of(table));
    ColumnDefinitions definitions =
        mockColumnDefinitions(
            mockColumnDefinition("start", BIGINT), mockColumnDefinition("end", BIGINT));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken(0, token1)).thenReturn(bs1);
    when(bs1.setToken(1, token2)).thenReturn(bs1);
    when(bs1.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs1);
    when(bs1.setRoutingToken(token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken(0, token2)).thenReturn(bs2);
    when(bs2.setToken(1, token3)).thenReturn(bs2);
    when(bs2.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs2);
    when(bs2.setRoutingToken(token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken(0, token3)).thenReturn(bs3);
    when(bs3.setToken(1, token1)).thenReturn(bs3);
    when(bs3.setRoutingKeyspace(any(CqlIdentifier.class))).thenReturn(bs3);
    when(bs3.setRoutingToken(token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM \\\"MyTable\\\" PER PARTITION LIMIT 1000\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<?> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM \"MyTable\" WHERE token(c1) > :start AND token(c1) <= :end PER PARTITION LIMIT 1000");
  }

  @Test
  void should_error_when_graph_and_keyspace_both_present() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "graph", "graph1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Settings schema.keyspace and schema.graph are mutually exclusive");
  }

  @Test
  void should_error_when_table_and_vertex_both_present() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "table", "t1", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Settings schema.table and schema.vertex are mutually exclusive");
  }

  @Test
  void should_error_when_table_and_edge_both_present() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "table", "t1", "edge", "e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Settings schema.table and schema.edge are mutually exclusive");
  }

  @Test
  void should_error_when_vertex_and_edge_both_present() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "vertex", "v1", "edge", "e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Settings schema.vertex and schema.edge are mutually exclusive");
  }

  @Test
  void should_error_when_edge_without_from_vertex() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "edge", "e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Setting schema.from is required when schema.edge is specified");
  }

  @Test
  void should_error_when_edge_without_to_vertex() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.schema", "edge", "e1", "from", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Setting schema.to is required when schema.edge is specified");
  }

  @Test
  void should_not_insert_where_clause_in_select_statement_if_already_exists() {
    ColumnDefinitions definitions = mockColumnDefinitions();
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs1);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "query", "\"SELECT a,b,c FROM t1 WHERE c1 = 1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    List<?> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(1);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT a,b,c FROM t1 WHERE c1 = 1");
  }

  @Test
  void should_warn_when_null_to_unset_true_and_protocol_version_lesser_than_4() {
    when(context.getProtocolVersion()).thenReturn(V3);
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    assertThat(recordMapper).isNotNull();
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Protocol version in use (%s) does not support unset bound variables; "
                    + "forcing schema.nullToUnset to false",
                V3));
  }

  @Test
  void should_error_when_both_indexed_and_mapped_mappings_unsupported() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Connector must support at least one of indexed or mapped mappings");
  }

  @Test
  void should_infer_insert_query_without_solr_query_column() {
    ColumnMetadata solrQueryCol = mock(ColumnMetadata.class);
    CqlIdentifier solrQueryColName = CqlIdentifier.fromInternal("solr_query");
    when(solrQueryCol.getName()).thenReturn(solrQueryColName);
    when(solrQueryCol.getType()).thenReturn(DataTypes.TEXT);
    when(table.getColumns())
        .thenReturn(ImmutableMap.of(C1, col1, C2, col2, C3, col3, solrQueryColName, solrQueryCol));
    IndexMetadata idx = mock(IndexMetadata.class);
    CqlIdentifier idxName = CqlIdentifier.fromInternal("idx");
    when(table.getIndexes()).thenReturn(ImmutableMap.of(idxName, idx));
    when(idx.getClassName())
        .thenReturn(Optional.of("com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1 (%1$s, \"%2$s\", %3$s) VALUES (:%1$s, :\"%2$s\", :%3$s)",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(mapper, "mapping"), C1, C1, C2, C2, C3, C3);
  }

  @Test
  void should_infer_select_query_without_solr_query_column() {
    ColumnMetadata solrQueryCol = mock(ColumnMetadata.class);
    CqlIdentifier solrQueryColName = CqlIdentifier.fromInternal("solr_query");
    when(solrQueryCol.getName()).thenReturn(solrQueryColName);
    when(solrQueryCol.getType()).thenReturn(DataTypes.TEXT);
    when(table.getColumns())
        .thenReturn(ImmutableMap.of(C1, col1, C2, col2, C3, col3, solrQueryColName, solrQueryCol));
    IndexMetadata idx = mock(IndexMetadata.class);
    CqlIdentifier idxName = CqlIdentifier.fromInternal("idx");
    when(table.getIndexes()).thenReturn(ImmutableMap.of(idxName, idx));
    when(idx.getClassName())
        .thenReturn(Optional.of("com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper mapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s, \"%2$s\", %3$s FROM ks.t1 WHERE token(%1$s) > :start AND token(%1$s) <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(mapper, "mapping"), C1, C1, C2, C2, C3, C3);
  }

  @Test
  void should_infer_select_query_with_solr_query_column_if_index_is_not_search_index() {
    ColumnMetadata solrQueryCol = mock(ColumnMetadata.class);
    CqlIdentifier solrQueryColName = CqlIdentifier.fromInternal("solr_query");
    when(solrQueryCol.getName()).thenReturn(solrQueryColName);
    when(solrQueryCol.getType()).thenReturn(DataTypes.TEXT);
    when(table.getColumns())
        .thenReturn(ImmutableMap.of(C1, col1, C2, col2, C3, col3, solrQueryColName, solrQueryCol));
    IndexMetadata idx = mock(IndexMetadata.class);
    CqlIdentifier idxName = CqlIdentifier.fromInternal("idx");
    when(table.getIndexes()).thenReturn(ImmutableMap.of(idxName, idx));
    when(idx.getClassName()).thenReturn(Optional.of("not a search index"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, false, true);
    ReadResultMapper mapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s, \"%2$s\", %3$s, solr_query FROM ks.t1 WHERE token(%1$s) > :start AND token(%1$s) <= :end",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(mapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C3,
        C3,
        "solr_query",
        "solr_query");
  }

  @Test
  void should_error_when_vertex_non_existent() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Vertex label v1 does not exist");
  }

  @Test
  void should_error_when_vertex_non_existent_but_lower_case_variant_exists() {
    DseVertexMetadata vertexMetadata = mock(DseVertexMetadata.class);
    when(table.getVertex()).thenAnswer(x -> Optional.of(vertexMetadata));
    when(vertexMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("v1"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "\"V1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Vertex label \"V1\" does not exist, however a vertex label v1 was found. Did you mean to use -v v1?");
  }

  @Test
  void should_error_when_edge_non_existent() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "graph", "ks", "edge", "e1", "from", "v1", "to", "v2");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Edge label e1 from v1 to v2 does not exist");
  }

  @Test
  void should_error_when_edge_non_existent_but_lower_case_variant_exists() {
    DseEdgeMetadata edgeMetadata = mock(DseEdgeMetadata.class);
    when(table.getEdge()).thenAnswer(x -> Optional.of(edgeMetadata));
    when(edgeMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("e1"));
    when(edgeMetadata.getFromLabel()).thenReturn(CqlIdentifier.fromInternal("v1"));
    when(edgeMetadata.getToLabel()).thenReturn(CqlIdentifier.fromInternal("V2"));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "graph", "ks", "edge", "\"E1\"", "from", "\"V1\"", "to", "\"V2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(
            () -> schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Edge label \"E1\" from \"V1\" to \"V2\" does not exist, however an edge label e1 from v1 to \"V2\" was found. Did you mean to use -e e1 -from v1 -to \"V2\"?");
  }

  @Test
  void should_locate_existing_vertex_label() {
    DseVertexMetadata vertexMetadata = mock(DseVertexMetadata.class);
    when(table.getVertex()).thenAnswer(v -> Optional.of(vertexMetadata));
    when(vertexMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("v1"));
    when(keyspace.getGraphEngine()).thenReturn(Optional.of("Core"));
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.schema", "graph", "ks", "vertex", "v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    assertThat(getInternalState(schemaSettings, "table")).isSameAs(table);
  }

  @Test
  void should_locate_existing_edge_label() {
    DseEdgeMetadata edgeMetadata = mock(DseEdgeMetadata.class);
    when(table.getEdge()).thenAnswer(x -> Optional.of(edgeMetadata));
    when(edgeMetadata.getLabelName()).thenReturn(CqlIdentifier.fromInternal("e1"));
    when(edgeMetadata.getFromLabel()).thenReturn(CqlIdentifier.fromInternal("v1"));
    when(edgeMetadata.getToLabel()).thenReturn(CqlIdentifier.fromInternal("v2"));
    when(keyspace.getGraphEngine()).thenReturn(Optional.of("Core"));
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "graph", "ks", "edge", "e1", "from", "v1", "to", "v2");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.MAP_AND_WRITE, session, true, false);
    assertThat(getInternalState(schemaSettings, "table")).isSameAs(table);
  }

  @Test
  void should_generate_single_read_statement_when_query_not_parallelizable() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.schema", "query", "\"select * from ks.t1 LIMIT 10\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(SchemaGenerationType.READ_AND_MAP, session, true, true);
    assertThat(getInternalState(schemaSettings, "query")).isEqualTo("select * from ks.t1 LIMIT 10");
  }

  private static void assertMapping(DefaultMapping mapping, Object... fieldsAndVars) {
    ImmutableSetMultimap.Builder<Object, Object> expected = ImmutableSetMultimap.builder();
    for (int i = 0; i < fieldsAndVars.length; i += 2) {
      String first =
          fieldsAndVars[i] instanceof String
              ? (String) fieldsAndVars[i]
              : ((CqlIdentifier) fieldsAndVars[i]).asInternal();
      CQLWord second =
          fieldsAndVars[i + 1] instanceof String
              ? CQLWord.fromInternal((String) fieldsAndVars[i + 1])
              : CQLWord.fromCqlIdentifier((CqlIdentifier) fieldsAndVars[i + 1]);
      if (CharMatcher.inRange('0', '9').matchesAllOf(first)) {
        expected.put(new DefaultIndexedField(Integer.parseInt(first)), second);
      } else {
        expected.put(new DefaultMappedField(first), second);
      }
    }
    @SuppressWarnings("unchecked")
    SetMultimap<Field, CQLWord> fieldsToVariables =
        (SetMultimap<Field, CQLWord>) getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).isEqualTo(expected.build());
  }
}
