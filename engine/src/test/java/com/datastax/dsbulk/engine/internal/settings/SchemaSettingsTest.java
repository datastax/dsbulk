/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newPreparedId;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newToken;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newTokenRange;
import static com.datastax.driver.core.DriverCoreHooks.wrappedStatement;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.commons.tests.utils.ReflectionUtils.getInternalState;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.EdgeMetadata;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.VertexMetadata;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.partitioner.TokenRangeReadStatement;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultIndexedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.CQLFragment;
import com.datastax.dsbulk.engine.internal.schema.CQLIdentifier;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
@ExtendWith(LogInterceptingExtension.class)
class SchemaSettingsTest {

  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";
  private static final String C4 = "c4";

  private final Token token1 = newToken(-9223372036854775808L);
  private final Token token2 = newToken(-3074457345618258603L);
  private final Token token3 = newToken(3074457345618258602L);

  private final Set<TokenRange> tokenRanges =
      Sets.newHashSet(
          newTokenRange(token1, token2),
          newTokenRange(token2, token3),
          newTokenRange(token3, token1));

  private final ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private final RecordMetadata recordMetadata = (field, cqlType) -> TypeToken.of(String.class);

  private final LogInterceptor logs;

  private Session session;
  private Cluster cluster;
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

  @BeforeEach
  void setUp() {
    session = mock(Session.class);
    cluster = mock(Cluster.class);
    metadata = mock(Metadata.class);
    keyspace = mock(KeyspaceMetadata.class);
    table = mock(TableMetadata.class);
    ps = mock(PreparedStatement.class);
    col1 = mock(ColumnMetadata.class);
    col2 = mock(ColumnMetadata.class);
    col3 = mock(ColumnMetadata.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    List<ColumnMetadata> columns = newArrayList(col1, col2, col3);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(V4);
    when(metadata.getKeyspace("ks")).thenReturn(keyspace);
    when(metadata.getKeyspaces()).thenReturn(singletonList(keyspace));
    when(metadata.getTokenRanges()).thenReturn(tokenRanges);
    when(metadata.newToken(token1.toString())).thenReturn(token1);
    when(metadata.newToken(token2.toString())).thenReturn(token2);
    when(metadata.newToken(token3.toString())).thenReturn(token3);
    when(metadata.getPartitioner()).thenReturn("Murmur3Partitioner");
    when(metadata.getReplicas(anyString(), any(Token.class))).thenReturn(Collections.emptySet());
    when(keyspace.getTable("t1")).thenReturn(table);
    when(keyspace.getTables()).thenReturn(singletonList(table));
    when(keyspace.getName()).thenReturn("ks");
    when(session.prepare(anyString())).thenReturn(ps);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1)).thenReturn(col1);
    when(table.getColumn(C2)).thenReturn(col2);
    when(table.getColumn(C3)).thenReturn(col3);
    when(table.getPrimaryKey()).thenReturn(singletonList(col1));
    when(table.getPartitionKey()).thenReturn(singletonList(col1));
    when(table.getKeyspace()).thenReturn(keyspace);
    when(table.getName()).thenReturn("t1");
    when(col1.getName()).thenReturn(C1);
    when(col2.getName()).thenReturn(C2);
    when(col3.getName()).thenReturn(C3);
    when(col1.getType()).thenReturn(varchar());
    when(col2.getType()).thenReturn(varchar());
    when(col3.getType()).thenReturn(varchar());
    ColumnDefinitions definitions =
        newColumnDefinitions(
            newDefinition(C1, varchar()),
            newDefinition(C2, varchar()),
            newDefinition(C3, varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    when(ps.getPreparedId()).thenReturn(newPreparedId(definitions, new int[] {0}, V4));
  }

  @Test
  void should_create_record_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_keyspace_and_counter_table_provided() {
    when(col1.getType()).thenReturn(counter());
    when(col2.getType()).thenReturn(counter());
    when(col3.getType()).thenReturn(counter());
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "UPDATE ks.t1 SET \"%2$s\"=\"%2$s\"+:\"%2$s\",%3$s=%3$s+:%3$s WHERE %1$s=:%1$s",
                C1, C2, C3));
    DefaultMapping mapping = (DefaultMapping) getInternalState(recordMapper, "mapping");
    assertMapping(mapping, C2, C2, C1, C1, C3, C3);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_fail_to_create_schema_settings_when_mapping_many_to_one() {
    LoaderConfig config = makeLoaderConfig("mapping = \" 0 = f1, 1 = f1\", keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid schema.mapping: the following variables are mapped to more than one field: f1. Please review schema.mapping for duplicates.");
  }

  @Test
  void should_create_record_mapper_when_mapping_ttl_and_timestamp() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format(
                    "mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s, 1=__ttl, 3=__timestamp \", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) "
                    + "USING TTL :%3$s AND TIMESTAMP :%4$s",
                C1, C2, INTERNAL_TTL_VARNAME, INTERNAL_TIMESTAMP_VARNAME));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1,
        "1",
        INTERNAL_TTL_VARNAME.asInternal(),
        "3",
        INTERNAL_TIMESTAMP_VARNAME.asInternal());
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_function() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" now() = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (now(),:%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "2", C1);
  }

  @Test
  void should_create_record_mapper_with_static_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1, queryTtl=30");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TTL 30", C1, C2));
  }

  @Test
  void should_create_record_mapper_with_static_timestamp() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=\"2017-01-02T00:00:01Z\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TIMESTAMP %3$s",
                C1,
                C2,
                instantToNumber(Instant.parse("2017-01-02T00:00:01Z"), MICROSECONDS, EPOCH)));
  }

  @Test
  void should_create_record_mapper_with_static_timestamp_and_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=\"2017-01-02T00:00:01Z\", queryTtl=25");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) "
                    + "USING TTL 25 AND TIMESTAMP %3$s",
                C1,
                C2,
                instantToNumber(Instant.parse("2017-01-02T00:00:01Z"), MICROSECONDS, EPOCH)));
  }

  @Test
  void should_create_record_mapper_when_using_custom_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("c1var", varchar()), newDefinition("c2var", varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "mapping = \"0 = c1var , 2 = c2var\", "
                + "query = \"INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)\", "
                + "nullToUnset = true");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)");
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), "0", "c1var", "2", "c2var");
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_is_a_list_and_indexed() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "1", C1);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_is_a_list_and_mapped() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_and_statement_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + String.format(
                    "query=\"insert into ks.t1 (%1$s,\\\"%2$s\\\") values (:%1$s,:\\\"%2$s\\\")\"",
                    C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.t1 (%1$s,\"%2$s\") values (:%1$s,:\"%2$s\")", C1, C2));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(%1$s,\"%2$s\",%3$s) VALUES (:%1$s,:\"%2$s\",:%3$s)",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(%3$s,%1$s,\"%2$s\") VALUES (:%3$s,:%1$s,:\"%2$s\")",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C2, C2, C4, C3);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s,%2$s) VALUES (:%1$s,:%2$s)", C1, C3));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1, C3, C3);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s) VALUES (:%1$s)", C1));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"), C1, C1);
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(%1$s,\"%2$s\",%3$s) VALUES (:%1$s,:\"%2$s\",:%3$s)",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
  }

  @Test
  void should_create_row_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\",%1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
  }

  @Test
  void should_create_row_mapper_when_mapping_is_a_list_and_indexed() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, true);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\",%1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "1", C1);
  }

  @Test
  void should_create_row_mapper_when_mapping_is_a_list_and_mapped() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\",%1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %3$s,%1$s,\"%2$s\" FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C2, C2, C4, C3);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,%2$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C3));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), C1, C1, C3, C3);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
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

  @Test
  void should_create_row_mapper_when_mapping_and_statement_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + String.format("query=\"select \\\"%2$s\\\",%1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "select \"%2$s\",%1$s from ks.t1 "
                    + "WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
  }

  @Test
  void should_create_row_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"));
  }

  @Test
  void should_create_row_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) getInternalState(readResultMapper, "mapping"));
  }

  @Test
  void should_use_default_writetime_var_name() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" *=*, f1 = __timestamp \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat((Set) getInternalState(mapping, "writeTimeVariables"))
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME.asVariable());
  }

  @Test
  void should_detect_writetime_var_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(
            newDefinition("c1", varchar()),
            newDefinition("c2", varchar()),
            newDefinition("c3", varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (c1,c2) VALUES (:c1,:c2) USING TIMESTAMP :c3\","
                + "mapping = \" f1 = c1 , f2 = c2 , f3 = c3 \" ");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat((Set) getInternalState(mapping, "writeTimeVariables")).containsOnly("c3");
  }

  @Test
  void should_detect_quoted_writetime_var_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(
            newDefinition("c1", varchar()),
            newDefinition("c2", varchar()),
            newDefinition("\"This is a quoted \\\" variable name\"", varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (c1,c2) VALUES (:c1,:c2) USING TTL 123 AND tImEsTaMp     :\\\"This is a quoted \\\"\\\" variable name\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat((Set) getInternalState(mapping, "writeTimeVariables"))
        .containsOnly(CQLIdentifier.fromInternal("This is a quoted \" variable name").asVariable());
  }

  @Test
  void should_include_function_call_in_insert_statement() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo("INSERT INTO ks.t1(c1,c3) VALUES (:c1,now())");
  }

  @Test
  void should_include_function_call_in_select_statement() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(getInternalState(schemaSettings, "query"))
        .isEqualTo("SELECT c1,now() FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_error_when_misplaced_function_call_in_insert_statement() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" f1 = c1, f2 = now() \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Misplaced function call detected on the right side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_error_when_misplaced_function_call_in_select_statement() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" f1 = c1, now() = c3 \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Misplaced function call detected on the left side of a mapping entry; "
                + "please review your schema.mapping setting");
  }

  @Test
  void should_create_single_read_statement_when_no_variables() {
    when(ps.getVariables()).thenReturn(newColumnDefinitions());
    BoundStatement bs = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs);
    LoaderConfig config = makeLoaderConfig("query = \"SELECT a,b,c FROM ks.t1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(cluster, 3);
    assertThat(statements).hasSize(1);
    assertThat(statements.get(0)).isEqualTo(bs);
  }

  @Test
  void should_create_multiple_read_statements() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(cluster, 3);
    assertThat(statements)
        .hasSize(3)
        .anySatisfy(
            // token range 1
            stmt -> {
              assertThat(stmt).isInstanceOf(TokenRangeReadStatement.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs1);
              assertThat(stmt.getRoutingToken()).isEqualTo(token2);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 2
            stmt -> {
              assertThat(stmt).isInstanceOf(TokenRangeReadStatement.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs2);
              assertThat(stmt.getRoutingToken()).isEqualTo(token3);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 3
            stmt -> {
              assertThat(stmt).isInstanceOf(TokenRangeReadStatement.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs3);
              assertThat(stmt.getRoutingToken()).isEqualTo(token1);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            });
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE token(a) > :start and token(a) <= :end \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(cluster, 3);
    assertThat(statements)
        .hasSize(3)
        .anySatisfy(
            // token range 1
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs1);
              assertThat(stmt.getRoutingToken()).isEqualTo(token2);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 2
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs2);
              assertThat(stmt.getRoutingToken()).isEqualTo(token3);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 3
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs3);
              assertThat(stmt.getRoutingToken()).isEqualTo(token1);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            });
  }

  @Test
  void should_create_multiple_read_statements_for_counting() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(cluster, 3);
    assertThat(statements)
        .hasSize(3)
        .anySatisfy(
            // token range 1
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs1);
              assertThat(stmt.getRoutingToken()).isEqualTo(token2);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 2
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs2);
              assertThat(stmt.getRoutingToken()).isEqualTo(token3);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 3
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs3);
              assertThat(stmt.getRoutingToken()).isEqualTo(token1);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            });
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query_for_counting() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT token(a) FROM t1 WHERE token(a) > :start and token(a) <= :end \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> statements = schemaSettings.createReadStatements(cluster, 3);
    assertThat(statements)
        .hasSize(3)
        .anySatisfy(
            // token range 1
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs1);
              assertThat(stmt.getRoutingToken()).isEqualTo(token2);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 2
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs2);
              assertThat(stmt.getRoutingToken()).isEqualTo(token3);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            })
        .anySatisfy(
            // token range 3
            stmt -> {
              assertThat(stmt).isInstanceOf(StatementWrapper.class);
              Statement wrapped = wrappedStatement((StatementWrapper) stmt);
              assertThat(wrapped).isInstanceOf(BoundStatement.class);
              BoundStatement bs = (BoundStatement) wrapped;
              assertThat(bs).isSameAs(bs3);
              assertThat(stmt.getRoutingToken()).isEqualTo(token1);
              assertThat(stmt.getKeyspace()).isEqualTo("ks");
            });
  }

  @Test
  void should_create_row_counter_for_global_stats() {
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2));
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(global), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT TTL(c3) FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_create_row_counter_for_global_stats_no_regular_column() {
    when(table.getPrimaryKey()).thenReturn(newArrayList(col1, col2, col3));
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(global), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_create_row_counter_for_partition_stats() {
    when(table.getClusteringColumns()).thenReturn(Collections.singletonList(col2));
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_create_row_counter_for_hosts_stats() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(hosts), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT token(c1) FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_create_row_counter_for_ranges_stats() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(ranges), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT token(c1) FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_create_row_counter_for_partitions_and_ranges_stats() {
    when(table.getClusteringColumns()).thenReturn(Collections.singletonList(col2));
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
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
    when(table.getClusteringColumns()).thenReturn(Collections.singletonList(col2));
    LoaderConfig config = makeLoaderConfig("query = \"SELECT c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT c1 FROM ks.t1 WHERE c1 = 0");
  }

  @Test
  void should_replace_select_clause_when_count_query_contains_extraneous_columns_partitions() {
    when(table.getClusteringColumns()).thenReturn(Collections.singletonList(col2));
    LoaderConfig config = makeLoaderConfig("query = \"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(partitions), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT c1 FROM ks.t1 WHERE c1 = 0");
  }

  @Test
  void should_replace_select_clause_when_count_query_contains_extraneous_columns_hosts() {
    LoaderConfig config = makeLoaderConfig("query = \"SELECT c1, c3 FROM ks.t1 WHERE c1 = 0\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
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
        newColumnDefinitions(
            newDefinition("My Start", bigint()), newDefinition("My End", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs1);
    when(bs1.setToken("\"My Start\"", token1)).thenReturn(bs1);
    when(bs1.setToken("\"My End\"", token2)).thenReturn(bs1);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE token(a) > :\\\"My Start\\\" and token(a) <= :\\\"My End\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM t1 WHERE token(a) > :\"My Start\" and token(a) <= :\"My End\"");
  }

  @Test
  void should_throw_configuration_exception_when_read_statement_variables_not_recognized() {
    ColumnDefinitions definitions = newColumnDefinitions(newDefinition("bar", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE foo = :bar\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(cluster, 3))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
                + "the WHERE clause is only allowed to contain one token range restriction of the form: "
                + "WHERE token(...) > :start AND token(...) <= :end");
  }

  @Test
  void should_throw_configuration_exception_when_read_statement_variables_not_recognized2() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("foo", bigint()), newDefinition("bar", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE token(a) >= :foo and token(a) < :bar \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(cluster, 3))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
                + "the WHERE clause is only allowed to contain one token range restriction of the form: "
                + "WHERE token(...) > :start AND token(...) <= :end");
  }

  @Test
  void should_warn_that_keyspace_was_not_found() {
    LoaderConfig config = makeLoaderConfig("keyspace = KS, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Keyspace \"KS\" does not exist, however a keyspace ks was found. Did you mean to use -k ks?");
  }

  @Test
  void should_warn_that_table_was_not_found() {
    when(keyspace.getTable("\"MyTable\"")).thenReturn(null);
    when(keyspace.getTable("mytable")).thenReturn(table);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = T1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Table \"T1\" does not exist, however a table t1 was found. Did you mean to use -t t1?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found_2() {
    when(metadata.getKeyspace("\"MyKs\"")).thenReturn(null);
    when(metadata.getKeyspace("myks")).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("keyspace = MyKs, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Keyspace \"MyKs\" does not exist");
  }

  @Test
  void should_warn_that_table_was_not_found_2() {
    when(keyspace.getTable("\"MyTable\"")).thenReturn(null);
    when(keyspace.getTable("mytable")).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Table \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mapped_fields_not_supported() {
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1, mapping = \"c1=c1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
  }

  @Test
  void should_error_when_graph_and_keyspace_both_present() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, graph=graph1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Settings schema.keyspace and schema.graph are mutually exclusive");
  }

  @Test
  void should_error_when_table_and_vertex_both_present() {
    LoaderConfig config = makeLoaderConfig("table=t1, vertex=v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Settings schema.table and schema.vertex are mutually exclusive");
  }

  @Test
  void should_error_when_table_and_edge_both_present() {
    LoaderConfig config = makeLoaderConfig("table=t1, edge=e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Settings schema.table and schema.edge are mutually exclusive");
  }

  @Test
  void should_error_when_vertex_and_edge_both_present() {
    LoaderConfig config = makeLoaderConfig("vertex=v1, edge=e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Settings schema.vertex and schema.edge are mutually exclusive");
  }

  @Test
  void should_error_when_edge_without_from_vertex() {
    LoaderConfig config = makeLoaderConfig("edge=e1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Setting schema.from is required when schema.edge is specified");
  }

  @Test
  void should_error_when_edge_without_to_vertex() {
    LoaderConfig config = makeLoaderConfig("edge=e1, from=v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Setting schema.to is required when schema.edge is specified");
  }

  @Test
  void should_error_invalid_schema_settings() {
    LoaderConfig config = makeLoaderConfig("");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then either schema.keyspace or schema.graph must be defined, and either schema.table, schema.vertex or schema.edge must be defined");
  }

  @Test
  void should_error_invalid_schema_mapping_missing_keyspace_and_table() {
    LoaderConfig config = makeLoaderConfig("mapping=\"c1=c2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "When schema.query is not defined, then either schema.keyspace or schema.graph must be defined, and either schema.table, schema.vertex or schema.edge must be defined");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_provided() {
    LoaderConfig config =
        makeLoaderConfig("query=\"INSERT INTO ks.t1 (col1) VALUES (?)\", keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_not_qualified_and_keyspace_not_provided() {
    LoaderConfig config = makeLoaderConfig("query=\"INSERT INTO t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
  }

  @Test
  void should_error_when_query_is_qualified_and_keyspace_non_existent() {
    when(metadata.getKeyspace("ks")).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("query=\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.query references a non-existent keyspace: ks");
  }

  @Test
  void should_error_when_query_is_provided_and_table_non_existent() {
    when(keyspace.getTable("t1")).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("query=\"INSERT INTO ks.t1 (col1) VALUES (?)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.query references a non-existent table: t1");
  }

  @Test
  void should_error_invalid_schema_query_with_ttl() {
    LoaderConfig config =
        makeLoaderConfig("query=\"INSERT INTO t1 (col1) VALUES (?)\", queryTtl=30, keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_timestamp() {
    LoaderConfig config =
        makeLoaderConfig(
            "query=\"INSERT INTO t1 (col1) VALUES (?)\", queryTimestamp=\"2018-05-18T15:00:00Z\", keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_timestamp() {
    LoaderConfig config =
        makeLoaderConfig(
            "query=\"INSERT INTO t1 (col1) VALUES (?)\", mapping = \"f1=__timestamp\", keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined when mapping a field to query-timestamp");
  }

  @Test
  void should_error_invalid_schema_query_with_keyspace_and_table() {
    LoaderConfig config =
        makeLoaderConfig(
            "query=\"INSERT INTO t1 (col1) VALUES (?)\", keyspace=keyspace, table=table");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined if schema.table, schema.vertex or schema.edge are defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            "query=\"INSERT INTO t1 (col1) VALUES (?)\", mapping = \"f1=__ttl\", keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.query must not be defined when mapping a field to query-ttl");
  }

  @Test
  void should_error_when_mapping_provided_and_count_workflow() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1, mapping = \"col1,col2\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.COUNT, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("schema.mapping must not be defined when counting rows in a table");
  }

  @Test
  void should_error_invalid_timestamp() {
    LoaderConfig config = makeLoaderConfig("queryTimestamp=junk, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Expecting schema.queryTimestamp to be in ISO_ZONED_DATE_TIME format but got 'junk'");
  }

  @Test
  void should_error_invalid_schema_missing_keyspace() {
    LoaderConfig config = makeLoaderConfig("table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.keyspace or schema.graph must be defined if schema.table, schema.vertex or schema.edge are defined");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present() {
    LoaderConfig config =
        makeLoaderConfig(
            "mapping=\"now() = c1, 0 = c2\", query=\"INSERT INTO t1 (col1) VALUES (?)\", keyspace=ks");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "schema.query must not be defined when mapping a function to a column");
  }

  @Test
  void should_throw_exception_when_nullToUnset_not_a_boolean() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("keyspace=ks, table=t1, nullToUnset = NotABoolean")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for schema.nullToUnset: Expecting BOOLEAN, got STRING");
  }

  @Test
  void should_error_when_vertex_non_existent() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, vertex=v1")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Vertex label v1 does not exist");
  }

  @Test
  void should_error_when_vertex_non_existent_but_lower_case_variant_exists() {
    VertexMetadata vertexMetadata = mock(VertexMetadata.class);
    when(table.getVertexMetadata()).thenReturn(vertexMetadata);
    when(vertexMetadata.getLabelName()).thenReturn("v1");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, vertex=\"V1\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Vertex label \"V1\" does not exist, however a vertex label v1 was found. Did you mean to use -v v1?");
  }

  @Test
  void should_error_when_edge_non_existent() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, edge=e1, from=v1, to=v2")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Edge label e1 from v1 to v2 does not exist");
  }

  @Test
  void should_error_when_edge_non_existent_but_lower_case_variant_exists() {
    EdgeMetadata edgeMetadata = mock(EdgeMetadata.class);
    when(table.getEdgeMetadata()).thenReturn(edgeMetadata);
    when(edgeMetadata.getLabelName()).thenReturn("e1");
    when(edgeMetadata.getFromLabel()).thenReturn("v1");
    when(edgeMetadata.getToLabel()).thenReturn("V2");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, edge=\"E1\", from=\"V1\", to=\"V2\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Edge label \"E1\" from \"V1\" to \"V2\" does not exist, however an edge label e1 from v1 to \"V2\" was found. Did you mean to use -e e1 -from v1 -to \"V2\"?");
  }

  @Test
  void should_locate_existing_vertex_label() {
    VertexMetadata vertexMetadata = mock(VertexMetadata.class);
    when(table.getVertexMetadata()).thenReturn(vertexMetadata);
    when(vertexMetadata.getLabelName()).thenReturn("v1");
    when(keyspace.getGraphEngine()).thenReturn("Native");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, vertex=v1")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, true);
    assertThat(getInternalState(schemaSettings, "table")).isSameAs(table);
  }

  @Test
  void should_locate_existing_edge_label() {
    EdgeMetadata edgeMetadata = mock(EdgeMetadata.class);
    when(table.getEdgeMetadata()).thenReturn(edgeMetadata);
    when(edgeMetadata.getLabelName()).thenReturn("e1");
    when(edgeMetadata.getFromLabel()).thenReturn("v1");
    when(edgeMetadata.getToLabel()).thenReturn("v2");
    when(keyspace.getGraphEngine()).thenReturn("Native");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("graph=ks, edge=e1, from=v1, to=v2")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, true);
    assertThat(getInternalState(schemaSettings, "table")).isSameAs(table);
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_column() {
    LoaderConfig config =
        makeLoaderConfig("keyspace=ks, table=t1, mapping = \"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Schema mapping entry 'nonExistentCol' doesn't match any column found in table t1");
  }

  @Test
  void should_error_when_mapping_contains_entry_that_does_not_match_any_bound_variable() {
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)\", mapping = \"fieldA = nonExistentCol\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Schema mapping entry 'nonExistentCol' doesn't match any bound variable found in query: 'INSERT INTO ks.t1 (c1, c2) VALUES (:c1, :c2)'");
  }

  @Test
  void should_error_when_mapping_does_not_contain_primary_key() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1, mapping = \"fieldA = c3\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void should_error_when_insert_query_does_not_contain_primary_key() {
    LoaderConfig config = makeLoaderConfig("query = \"INSERT INTO ks.t1 (c2) VALUES (:c2)\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Missing required primary key column c1 from schema.mapping or schema.query");
  }

  @Test
  void should_error_when_counting_partitions_but_table_has_no_clustering_column() {
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, cluster, false);
    assertThatThrownBy(
            () ->
                schemaSettings.createReadResultCounter(
                    session, codecRegistry, EnumSet.of(partitions), 10))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Cannot count partitions for table t1: it has no clustering column.");
  }

  @Test
  void should_error_when_graph_options_provided_but_cluster_is_not_compatible() {
    VertexMetadata vertexMetadata = mock(VertexMetadata.class);
    when(table.getVertexMetadata()).thenReturn(vertexMetadata);
    when(vertexMetadata.getLabelName()).thenReturn("v1");
    Host host = mock(Host.class);
    when(metadata.getAllHosts()).thenReturn(Collections.singleton(host));
    when(host.getDseVersion()).thenReturn(VersionNumber.parse("6.0.0"));
    when(host.toString()).thenReturn("host1");
    LoaderConfig config = makeLoaderConfig("graph=ks, vertex=v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
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
    VertexMetadata vertexMetadata = mock(VertexMetadata.class);
    when(table.getVertexMetadata()).thenReturn(vertexMetadata);
    when(vertexMetadata.getLabelName()).thenReturn("v1");
    LoaderConfig config = makeLoaderConfig("graph=ks, vertex=v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Graph operations requested but provided keyspace is not a graph: ks");
  }

  @Test
  void should_error_when_graph_options_provided_but_keyspace_not_native_graph() {
    VertexMetadata vertexMetadata = mock(VertexMetadata.class);
    when(table.getVertexMetadata()).thenReturn(vertexMetadata);
    when(vertexMetadata.getLabelName()).thenReturn("v1");
    when(keyspace.getGraphEngine()).thenReturn("Legacy");
    LoaderConfig config = makeLoaderConfig("graph=ks, vertex=v1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    assertThatThrownBy(() -> schemaSettings.init(WorkflowType.LOAD, cluster, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Graph operations requested but provided graph ks was created with an unsupported graph engine: Legacy");
  }

  @Test
  void should_warn_when_keyspace_is_native_graph_but_non_graph_options_provided() {
    when(keyspace.getGraphEngine()).thenReturn("Native");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, true);
    assertThat(logs)
        .hasMessageContaining(
            "Provided keyspace is a graph; "
                + "instead of schema.keyspace and schema.table, please use graph-specific options "
                + "such as schema.graph, schema.vertex, schema.edge, schema.from and schema.to.");
  }

  @Test
  void should_warn_when_keyspace_is_legacy_graph_and_workflow_is_load() {
    when(keyspace.getGraphEngine()).thenReturn("Legacy");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, cluster, true);
    assertThat(logs)
        .hasMessageContaining(
            "Provided keyspace is a graph created with a legacy graph engine: "
                + "Legacy; attempting to load data into such a keyspace is not supported and "
                + "may put the graph in an inconsistent state.");
  }

  @Test
  void should_insert_where_clause_in_select_statement_simple() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, query = \"SELECT a,b,c FROM t1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(cluster, 3);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT a,b,c FROM t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_insert_where_clause_in_select_statement_complex() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, query = \"SELECT a,b,c FROM t1 LIMIT 1000\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(cluster, 3);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM t1 WHERE token(c1) > :start AND token(c1) <= :end LIMIT 1000");
  }

  @Test
  void should_insert_where_clause_in_select_statement_case_sensitive() {
    when(keyspace.getTable("\"MyTable\"")).thenReturn(table);
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", bigint()), newDefinition("end", bigint()));
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, query = \"SELECT a,b,c FROM \\\"MyTable\\\" LIMIT 1000\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(cluster, 3);
    assertThat(stmts).hasSize(3);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            "SELECT a,b,c FROM \"MyTable\" WHERE token(c1) > :start AND token(c1) <= :end LIMIT 1000");
  }

  @Test
  void should_not_insert_where_clause_in_select_statement_if_already_exists() {
    ColumnDefinitions definitions = newColumnDefinitions();
    when(ps.getVariables()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs1);
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE c1 = 1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, cluster, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<? extends Statement> stmts = schemaSettings.createReadStatements(cluster, 3);
    assertThat(stmts).hasSize(1);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("SELECT a,b,c FROM t1 WHERE c1 = 1");
  }

  @NotNull
  private static LoaderConfig makeLoaderConfig(String configString) {
    return new DefaultLoaderConfig(
        ConfigFactory.parseString(configString)
            .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
  }

  private static void assertMapping(DefaultMapping mapping) {
    assertMapping(mapping, C1, C1, C2, C2, C3, C3);
  }

  private static void assertMapping(DefaultMapping mapping, String... fieldsAndVars) {
    Multimap<Field, CQLFragment> expected = ArrayListMultimap.create();
    for (int i = 0; i < fieldsAndVars.length; i += 2) {
      String first = fieldsAndVars[i];
      String second = fieldsAndVars[i + 1];
      if (CharMatcher.inRange('0', '9').matchesAllOf(first)) {
        expected.put(
            new DefaultIndexedField(Integer.parseInt(first)), CQLIdentifier.fromInternal(second));
      } else {
        expected.put(new DefaultMappedField(first), CQLIdentifier.fromInternal(second));
      }
    }
    Multimap<Field, CQLFragment> fieldsToVariables =
        (Multimap<Field, CQLFragment>) getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).isEqualTo(expected);
  }
}
