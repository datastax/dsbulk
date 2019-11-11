/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockColumnDefinition;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockColumnDefinitions;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockSession;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.newToken;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.newTokenRange;
import static com.datastax.dsbulk.commons.tests.utils.ReflectionUtils.getInternalState;
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.INTERNAL;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V3;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.COUNTER;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
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

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultIndexedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.schema.CQLWord;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  private final ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private final RecordMetadata recordMetadata = (field, cqlType) -> GenericType.STRING;

  private final LogInterceptor logs;

  private CqlSession session;
  private DriverContext context;
  private Metadata metadata;
  private KeyspaceMetadata keyspace;
  private TableMetadata table;
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
    keyspace = mock(KeyspaceMetadata.class);
    table = mock(TableMetadata.class);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "mapping", "\" 0 = f1, 1 = f1\"", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid schema.mapping: the following variables are mapped to more than one field: f1. Please review schema.mapping for duplicates.");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_record_mapper_when_mapping_ttl_and_timestamp(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" now() = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "keyspace",
            "ks",
            "table",
            "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"0 = c1var , 2 = c2var\"",
            "query",
            "\"INSERT INTO ks.t1 (c2, c1) VALUES (:c2var, :c1var)\"",
            "nullToUnset",
            true);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "query",
            String.format(
                "\"insert into ks.t1 (%1$s,\\\"%2$s\\\") values (:%1$s, :\\\"%2$s\\\")\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, true, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, true, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "nullToUnset", false, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
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
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            String.format("\" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2),
            "nullToUnset",
            true,
            "query",
            String.format("\"select \\\"%2$s\\\", %1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, true, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "nullToUnset", false, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" *=*, f1 = __timestamp \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1,c2) VALUES (:c1, :c2) USING TIMESTAMP :c3\"",
            "mapping",
            "\" f1 = c1 , f2 = c2 , f3 = c3 \" ");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1,c2) VALUES (:c1, :c2) USING TTL 123 AND tImEsTaMp     :\\\"This is a quoted \\\"\\\" variable name\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo("INSERT INTO ks.t1 (c1, c3) VALUES (:c1, now())");
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_include_function_call_in_select_statement(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo(
            "SELECT c1, now() AS \"now()\" FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_error_when_misplaced_function_call_in_insert_statement() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Misplaced function call detected on the right side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_error_when_misplaced_function_call_in_select_statement() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(UNLOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Misplaced function call detected on the left side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_create_single_read_statement_when_no_variables() {
    when(ps.getVariableDefinitions()).thenReturn(mockColumnDefinitions());
    BoundStatement bs = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs);
    LoaderConfig config = createTestConfig("dsbulk.schema", "query", "\"SELECT a,b,c FROM ks.t1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(session);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) > :start and token(a) <= :end \"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) <= ? AND token(a) > ?\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement<?>> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(3).contains(bs1, bs2, bs3);
  }

  @ParameterizedTest
  @MethodSource("allProtocolVersions")
  void should_create_row_counter_for_global_stats(ProtocolVersion version) {
    when(context.getProtocolVersion()).thenReturn(version);
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2));
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(global), 10);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(hosts), 10);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(ranges), 10);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(
            session, codecRegistry, EnumSet.of(partitions, ranges), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_replace_select_clause_when_count_query_does_not_contain_partition_key() {
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(col2, ClusteringOrder.ASC));
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"SELECT c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT c1 FROM ks.t1 WHERE c1 = 0");
  }

  @Test
  void should_replace_select_clause_when_count_query_contains_extraneous_columns_partitions() {
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(col2, ClusteringOrder.ASC));
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT c1 FROM ks.t1 WHERE c1 = 0");
  }

  @Test
  void should_replace_select_clause_when_count_query_contains_extraneous_columns_hosts() {
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(
            session, codecRegistry, EnumSet.of(hosts, ranges), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT token(c1) FROM ks.t1 WHERE c1 = 0");
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) > :\\\"My Start\\\" and token(a) <= :\\\"My End\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE foo = :bar\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(session))
        .isInstanceOf(BulkConfigurationException.class)
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 WHERE token(a) >= :foo and token(a) < :bar \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(session))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
                + "the WHERE clause is only allowed to contain one token range restriction of the form: "
                + "WHERE token(...) > ? AND token(...) <= ?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found_but_similar_ks_exists() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "KS", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Keyspace \"KS\" does not exist, however a keyspace ks was found. Did you mean to use -k ks?");
  }

  @Test
  void should_warn_that_table_was_not_found_but_similar_table_exists() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "T1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Table \"T1\" does not exist, however a table t1 was found. Did you mean to use -t t1?");
  }

  @Test
  void should_warn_that_table_was_not_found_but_similar_mv_exists() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MV1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(UNLOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Table or materialized view \"MV1\" does not exist, however a materialized view mv1 was found. Did you mean to use -t mv1?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "MyKs", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Keyspace \"MyKs\" does not exist");
  }

  @Test
  void should_warn_that_table_was_not_found() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Table \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mv_was_not_found() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(UNLOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Table or materialized view \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mapped_fields_not_supported() {
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"c1=c1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
  }

  @Test
  void should_error_invalid_schema_settings() {
    LoaderConfig config = createTestConfig("dsbulk.schema");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_invalid_schema_mapping_missing_keyspace_and_table() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "mapping", "\"c1=c2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_provided() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"", "keyspace", "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_not_qualified_and_keyspace_not_provided() {
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"INSERT INTO t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_non_existent() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("ks"))).thenReturn(Optional.empty());
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.query references a non-existent keyspace: ks");
  }

  @Test
  void should_error_when_query_is_provided_and_table_non_existent() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("t1"))).thenReturn(Optional.empty());
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query references a non-existent table or materialized view: t1");
  }

  @Test
  void should_error_invalid_schema_query_with_ttl() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "queryTtl",
            30,
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_timestamp() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "queryTimestamp",
            "\"2018-05-18T15:00:00Z\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_timestamp() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "mapping",
            "\"f1=__timestamp\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "__timestamp variable is not allowed when schema.query does not contain a USING TIMESTAMP clause");
  }

  @Test
  void should_error_invalid_schema_query_with_keyspace_and_table() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "keyspace",
            "keyspace",
            "table",
            "table");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.query must not be defined if schema.table is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_ttl() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "mapping",
            "\"f1=__ttl\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "__ttl variable is not allowed when schema.query does not contain a USING TTL clause");
  }

  @Test
  void should_error_when_mapping_provided_and_count_workflow() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"col1,col2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.COUNT, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.mapping must not be defined when counting rows in a table");
  }

  @Test
  void should_error_invalid_timestamp() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "queryTimestamp", "junk", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Expecting schema.queryTimestamp to be in ISO_ZONED_DATE_TIME format but got 'junk'");
  }

  @Test
  void should_error_invalid_schema_missing_keyspace() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.keyspace must be defined if schema.table is defined");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present_load() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"now() = c1, 0 = c2\"",
            "query",
            "\"INSERT INTO t1 (col1) VALUES (?)\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Setting schema.query must not be defined when loading "
                + "if schema.mapping contains a function on the left side of a mapping entry");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present_unload() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "mapping",
            "\"f1 = now(), f2 = c2\"",
            "query",
            "\"SELECT c1, c2 FROM t1\"",
            "keyspace",
            "ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(UNLOAD, session, false, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Setting schema.query must not be defined when unloading "
                + "if schema.mapping contains a function on the right side of a mapping entry");
  }

  @Test
  void should_throw_exception_when_nullToUnset_not_a_boolean() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "nullToUnset", "NotABoolean");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, true, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.schema.nullToUnset, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_column() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "table",
            "t1",
            "mapping",
            "\"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Schema mapping entry \"nonExistentCol\" doesn't match any column found in table t1");
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_bound_variable() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "query",
            "\"INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)\"",
            "mapping",
            "\"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Schema mapping entry \"nonExistentCol\" doesn't match any bound variable found in query: 'INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)'");
  }

  @Test
  void should_error_when_mapping_does_not_contain_primary_key() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "table", "t1", "mapping", "\"fieldA = c3\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void should_error_when_insert_query_does_not_contain_primary_key() {
    when(table.getColumn(CqlIdentifier.fromInternal("c2"))).thenReturn(Optional.of(col2));
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "query", "\"INSERT INTO ks.t1 (c2) VALUES (:c2)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void
      should_not_error_when_insert_query_does_not_contain_clustering_column_but_mutation_is_static_only() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "query", "\"INSERT INTO ks.t1 (c1, c3) VALUES (:c1, :c3)\"");
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2));
    when(table.getPartitionKey()).thenReturn(singletonList(col1));
    when(col3.isStatic()).thenReturn(true);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
  }

  @Test
  void should_error_when_counting_partitions_but_table_has_no_clustering_column() {
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, session, false, true);
    assertThatThrownBy(
            () ->
                schemaSettings.createReadResultCounter(
                    session, codecRegistry, EnumSet.of(partitions), 10))
        .isInstanceOf(BulkConfigurationException.class)
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "query", "\"SELECT a,b,c FROM t1\"", "splits", 3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(session);
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM t1 LIMIT 1000\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM t1 WHERE token(c1) > :start AND token(c1) <= :end LIMIT 1000");
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema",
            "keyspace",
            "ks",
            "query",
            "\"SELECT a,b,c FROM \\\"MyTable\\\" LIMIT 1000\"",
            "splits",
            3);
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM \"MyTable\" WHERE token(c1) > :start AND token(c1) <= :end LIMIT 1000");
  }

  @Test
  void should_not_insert_where_clause_in_select_statement_if_already_exists() {
    ColumnDefinitions definitions = mockColumnDefinitions();
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs1);
    LoaderConfig config =
        createTestConfig(
            "dsbulk.schema", "keyspace", "ks", "query", "\"SELECT a,b,c FROM t1 WHERE c1 = 1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(session);
    assertThat(stmts).hasSize(1);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT a,b,c FROM t1 WHERE c1 = 1");
  }

  @Test
  void should_warn_when_null_to_unset_true_and_protocol_version_lesser_than_4() {
    when(context.getProtocolVersion()).thenReturn(V3);
    LoaderConfig config =
        createTestConfig("dsbulk.schema", "nullToUnset", true, "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(LOAD, session, false, false))
        .isInstanceOf(BulkConfigurationException.class)
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(LOAD, session, false, true);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper mapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    LoaderConfig config = createTestConfig("dsbulk.schema", "keyspace", "ks", "table", "t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(UNLOAD, session, false, true);
    ReadResultMapper mapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
