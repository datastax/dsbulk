/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DriverCoreEngineTestHooks.newPreparedId;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newToken;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTokenRange;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.google.common.collect.Lists.newArrayList;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class SchemaSettingsTest {

  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";
  private static final String C4 = "c4";

  private static final CqlIdentifier C1_IDENT = CqlIdentifier.fromInternal(C1);
  private static final CqlIdentifier C2_IDENT = CqlIdentifier.fromInternal(C2);
  private static final CqlIdentifier C3_IDENT = CqlIdentifier.fromInternal(C3);

  private static final CqlIdentifier KS_ID = CqlIdentifier.fromCql("ks");
  private static final CqlIdentifier TABLE_ID  = CqlIdentifier.fromCql("t1");

  private final Token token1 = newToken(-9223372036854775808L);
  private final Token token2 = newToken(-3074457345618258603L);
  private final Token token3 = newToken(3074457345618258602L);
  private final Set<TokenRange> tokenRanges =
      Sets.newHashSet(
          newTokenRange(token1, token2),
          newTokenRange(token2, token3),
          newTokenRange(token3, token1));

  private CqlSession session;
  private Metadata metadata;
  private KeyspaceMetadata keyspace;
  private TableMetadata table;
  private PreparedStatement ps;
  private ColumnMetadata col1;
  private ColumnMetadata col2;
  private ColumnMetadata col3;

  private final ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private final RecordMetadata recordMetadata = (field, cqlType) -> GenericType.of(String.class);

  @BeforeEach
  void setUp() {
    session = mock(CqlSession.class);
    metadata = mock(Metadata.class);
    keyspace = mock(KeyspaceMetadata.class);
    table = mock(TableMetadata.class);
    ps = mock(PreparedStatement.class);
    col1 = mock(ColumnMetadata.class);
    col2 = mock(ColumnMetadata.class);
    col3 = mock(ColumnMetadata.class);

    DriverContext configuration = mock(DriverContext.class);
//    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    Map<CqlIdentifier, ColumnMetadata> columns = ImmutableMap.<CqlIdentifier, ColumnMetadata>builder()
        .put(C1_IDENT, col1)
        .put(C2_IDENT, col2)
        .put(C3_IDENT, col3)
        .build();
    when(session.getMetadata()).thenReturn(metadata);
    when(session.getContext()).thenReturn(configuration);
//    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
//    when(protocolOptions.getProtocolVersion()).thenReturn(V4);
    when(metadata.getKeyspace(KS_ID)).thenReturn(keyspace);
//    when(metadata.getTokenRanges()).thenReturn(tokenRanges);
    when(keyspace.getTable(TABLE_ID)).thenReturn(table);
    when(session.prepare(anyString())).thenReturn(ps);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1)).thenReturn(col1);
    when(table.getColumn(C2)).thenReturn(col2);
    when(table.getColumn(C3)).thenReturn(col3);
    when(table.getPrimaryKey()).thenReturn(Collections.singletonList(col1));
    when(table.getPartitionKey()).thenReturn(Collections.singletonList(col1));
    when(col1.getName()).thenReturn(C1_IDENT);
    when(col2.getName()).thenReturn(C2_IDENT);
    when(col3.getName()).thenReturn(C3_IDENT);
    when(col1.getType()).thenReturn(DataTypes.TEXT);
    when(col2.getType()).thenReturn(DataTypes.TEXT);
    when(col3.getType()).thenReturn(DataTypes.TEXT);
    ColumnDefinitions definitions = newColumnDefinitions(C1_IDENT, C2_IDENT, C3_IDENT);
    when(ps.getVariableDefinitions()).thenReturn(definitions);
//    when(ps.getPreparedId()).thenReturn(newPreparedId(definitions, new int[] {0}, V4));
  }

  @Test
  void should_create_record_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_keyspace_and_counter_table_provided() {
    when(col1.getType()).thenReturn(DataTypes.COUNTER);
    when(col2.getType()).thenReturn(DataTypes.COUNTER);
    when(col3.getType()).thenReturn(DataTypes.COUNTER);
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
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
    DefaultMapping mapping =
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping");
    assertMapping(mapping, C2, C2, C1, C1, C3, C3);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_fail_to_create_schema_settings_when_mapping_many_to_one() {
    LoaderConfig config = makeLoaderConfig("mapping = \" 0 = f1, 1 = f1\", keyspace=ks, table=t1");
    assertThrows(
        BulkConfigurationException.class,
        () -> {
          SchemaSettings schemaSettings = new SchemaSettings(config);
          schemaSettings.init(WorkflowType.LOAD, false);
        },
        "Multiple input values in mapping resolve to column f1");
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
    schemaSettings.init(WorkflowType.LOAD, false);
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
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1,
        "1",
        INTERNAL_TTL_VARNAME,
        "3",
        INTERNAL_TIMESTAMP_VARNAME);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_function() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" now() = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (now(),:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"), "2", C1);
  }

  @Test
  void should_create_record_mapper_with_static_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "keyspace=ks, table=t1, queryTtl=30");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
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
    schemaSettings.init(WorkflowType.LOAD, false);
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
    schemaSettings.init(WorkflowType.LOAD, false);
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
        newColumnDefinitions(CqlIdentifier.fromInternal("c1var"), CqlIdentifier.fromInternal("c2var"));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "mapping = \"0 = c1var , 2 = c2var\", "
                + "query = \"INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)\", "
                + "nullToUnset = true");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)");
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        "c1var",
        "2",
        "c2var");
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_is_a_list_and_indexed() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, true);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "1",
        C1);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_mapping_is_a_list_and_mapped() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"), C1, C1, C2, C2);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
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
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.t1 (%1$s,\"%2$s\") values (:%1$s,:\"%2$s\")", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
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
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s,%2$s) VALUES (:%1$s,:%2$s)", C1, C3));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"), C1, C1, C3, C3);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s) VALUES (:%1$s)", C1));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"), C1, C1);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
  }

  @Test
  void should_create_record_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
  }

  @Test
  void should_create_row_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
  }

  @Test
  void should_create_row_mapper_when_mapping_is_a_list_and_indexed() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, true);
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
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        "0",
        C2,
        "1",
        C1);
  }

  @Test
  void should_create_row_mapper_when_mapping_is_a_list_and_mapped() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C2,
        C2);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=*, %1$s = %2$s \"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=-\\\"%1$s\\\" \"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C3,
        C3);
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \" *=[-\\\"%1$s\\\", -%2$s] \"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end", C1));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"), C1, C1);
  }

  @Test
  void should_create_row_mapper_when_mapping_and_statement_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \" 0 = \\\"%2$s\\\" , 2 = %1$s \", ", C1, C2)
                + "nullToUnset = true, "
                + String.format("query=\"select \\\"%2$s\\\",%1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("select \"%2$s\",%1$s from ks.t1", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
  }

  @Test
  void should_create_row_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"));
  }

  @Test
  void should_create_row_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"));
  }

  @Test
  void should_use_default_writetime_var_name() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \" *=*, f1 = __timestamp \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable"))
        .isEqualTo(CqlIdentifier.fromInternal(INTERNAL_TIMESTAMP_VARNAME));
  }

  @Test
  void should_detect_writetime_var_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(C1_IDENT, CqlIdentifier.fromInternal("c2"), C3_IDENT);
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (c1,c2) VALUES (:c1,:c2) USING TIMESTAMP :c3\","
                + "mapping = \" f1 = c1 , f2 = c2 , f3 = c3 \" ");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable")).isEqualTo(CqlIdentifier.fromInternal("c3"));
  }

  @Test
  void should_detect_quoted_writetime_var_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(C1_IDENT, CqlIdentifier.fromInternal("c2"), CqlIdentifier.fromCql("\"This is a quoted \\\" variable name\""));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (c1,c2) VALUES (:c1,:c2) USING TTL 123 AND tImEsTaMp     :\\\"This is a quoted \\\"\\\" variable name\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable"))
        .isEqualTo(CqlIdentifier.fromInternal("This is a quoted \" variable name"));
  }

  @Test
  void should_create_single_read_statement_when_no_variables() {
    when(ps.getVariableDefinitions()).thenReturn(newColumnDefinitions());
    BoundStatement bs = mock(BoundStatement.class);
    when(ps.bind()).thenReturn(bs);
    LoaderConfig config = makeLoaderConfig("query = \"SELECT a,b,c FROM ks.t1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement> statements = schemaSettings.createReadStatements(session);
    assertThat(statements).hasSize(1).containsExactly(bs);
  }

  @Test
  void should_create_multiple_read_statements() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", DataTypes.BIGINT, 0), newDefinition("end", DataTypes.BIGINT, 1));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    when(bs1.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    when(bs2.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(bs3.getKeyspace()).thenReturn(KS_ID);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement> statements = schemaSettings.createReadStatements(session);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
            });
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", DataTypes.BIGINT, 0), newDefinition("end", DataTypes.BIGINT, 1));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    when(bs1.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    when(bs2.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(bs3.getKeyspace()).thenReturn(KS_ID);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE token(a) > :start and token(a) <= :end \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement> statements = schemaSettings.createReadStatements(session);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
            });
  }

  @Test
  void should_create_multiple_read_statements_for_counting() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", DataTypes.BIGINT, 0), newDefinition("end", DataTypes.BIGINT, 1));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    when(bs1.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    when(bs2.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(bs3.getKeyspace()).thenReturn(KS_ID);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement> statements = schemaSettings.createReadStatements(session);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
            });
  }

  @Test
  void should_create_multiple_read_statements_when_token_range_provided_in_query_for_counting() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("start", DataTypes.BIGINT, 0), newDefinition("end", DataTypes.BIGINT, 1));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(bs1.setToken("start", token1)).thenReturn(bs1);
    when(bs1.setToken("end", token2)).thenReturn(bs1);
    when(bs1.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(bs2.setToken("start", token2)).thenReturn(bs2);
    when(bs2.setToken("end", token3)).thenReturn(bs2);
    when(bs2.getKeyspace()).thenReturn(KS_ID);
    BoundStatement bs3 = mock(BoundStatement.class);
    when(bs3.setToken("start", token3)).thenReturn(bs3);
    when(bs3.setToken("end", token1)).thenReturn(bs3);
    when(bs3.getKeyspace()).thenReturn(KS_ID);
    when(ps.bind()).thenReturn(bs1, bs2, bs3);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT token(a) FROM t1 WHERE token(a) > :start and token(a) <= :end \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    List<Statement> statements = schemaSettings.createReadStatements(session);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
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
              assertThat(stmt.getKeyspace()).isEqualTo(KS_ID);
            });
  }

  @Test
  void should_create_row_counter() {
    when(table.getPartitionKey()).thenReturn(newArrayList(col1));
    LoaderConfig config = makeLoaderConfig("keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.COUNT, false);
    ReadResultCounter counter =
        schemaSettings.createReadResultCounter(session, codecRegistry, EnumSet.of(global), 10);
    assertThat(counter).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1 FROM ks.t1 WHERE token(c1) > :start AND token(c1) <= :end");
  }

  @Test
  void should_throw_configuration_exception_when_read_statement_variables_not_recognized() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("foo", DataTypes.BIGINT, 0), newDefinition("bar", DataTypes.BIGINT, 1));
    when(ps.getVariableDefinitions()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "keyspace = ks, query = \"SELECT a,b,c FROM t1 WHERE token(a) > :foo and token(a) <= :bar \"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.UNLOAD, false);
    schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThatThrownBy(() -> schemaSettings.createReadStatements(session))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "The provided statement (schema.query) contains unrecognized bound variables: [foo, bar]; "
                + "only 'start' and 'end' can be used to define a token range");
  }

  @Test
  void should_warn_that_keyspace_was_not_found() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(null);
    when(metadata.getKeyspace("myks")).thenReturn(keyspace);
    LoaderConfig config = makeLoaderConfig("keyspace = MyKs, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Keyspace \"MyKs\" does not exist, however a keyspace myks was found. Did you mean to use -k myks?");
  }

  @Test
  void should_warn_that_table_was_not_found() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(null);
    when(keyspace.getTable("mytable")).thenReturn(table);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Table \"MyTable\" does not exist, however a table mytable was found. Did you mean to use -t mytable?");
  }

  @Test
  void should_warn_that_keyspace_was_not_found_2() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(null);
    when(metadata.getKeyspace("myks")).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("keyspace = MyKs, table = t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Keyspace \"MyKs\" does not exist");
  }

  @Test
  void should_warn_that_table_was_not_found_2() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(null);
    when(keyspace.getTable(CqlIdentifier.fromInternal("mytable"))).thenReturn(null);
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = MyTable");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, false);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table \"MyTable\" does not exist");
  }

  @Test
  void should_warn_that_mapped_fields_not_supported() {
    LoaderConfig config = makeLoaderConfig("keyspace = ks, table = t1, mapping = \"c1=c1\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(WorkflowType.LOAD, true);
    assertThatThrownBy(
            () -> schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
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
    Map<String, CqlIdentifier> expected = new HashMap<>();
    for (int i = 0; i < fieldsAndVars.length; i += 2) {
      String first = fieldsAndVars[i];
      String second = fieldsAndVars[i + 1];
      expected.put(first, CqlIdentifier.fromInternal(second));
    }
    Map<String, CqlIdentifier> fieldsToVariables =
        (Map<String, CqlIdentifier>) ReflectionUtils.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).isEqualTo(expected);
  }

  private static ColumnDefinitions newColumnDefinitions(CqlIdentifier... columns)
  {
    List<ColumnDefinition> defs = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      defs.add(newDefinition(columns[i], DataTypes.TEXT, i));
    }
    return DefaultColumnDefinitions.valueOf(defs);
  }

  private static ColumnDefinitions newColumnDefinitions(ColumnDefinition... defs) {
    return DefaultColumnDefinitions.valueOf(Arrays.asList(defs));
  }
  
  private static ColumnDefinition newDefinition(CqlIdentifier name, DataType type, int index)
  {
    return newDefinition(name.asInternal(), type, index);
  }

  private static ColumnDefinition newDefinition(String name, DataType type, int index)
  {
    return new DefaultColumnDefinition(new ColumnSpec("ks", "t1", name, index, RawType.PRIMITIVES.get(type.getProtocolCode())), null);
  }

}
