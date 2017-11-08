/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DriverCoreTestHooks.newPreparedId;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TTL_VARNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.SchemaFreeRecordMetadata;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;

/** */
@SuppressWarnings("unchecked")
public class SchemaSettingsTest {

  private static final String NULL_STRINGS = "nullStrings";
  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";
  private static final String C4 = "c4";

  private Session session;
  private final ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private final RecordMetadata recordMetadata = new SchemaFreeRecordMetadata();

  @Before
  public void setUp() throws Exception {
    session = mock(Session.class);
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    TableMetadata table = mock(TableMetadata.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ColumnMetadata col1 = mock(ColumnMetadata.class);
    ColumnMetadata col2 = mock(ColumnMetadata.class);
    ColumnMetadata col3 = mock(ColumnMetadata.class);
    List<ColumnMetadata> columns = Lists.newArrayList(col1, col2, col3);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(keyspace);
    when(keyspace.getTable(anyString())).thenReturn(table);
    when(session.prepare(anyString())).thenReturn(ps);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1)).thenReturn(col1);
    when(table.getColumn(C2)).thenReturn(col2);
    when(table.getColumn(C3)).thenReturn(col3);
    when(col1.getName()).thenReturn(C1);
    when(col2.getName()).thenReturn(C2);
    when(col3.getName()).thenReturn(C3);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    when(ps.getVariables()).thenReturn(definitions);
    when(definitions.size()).thenReturn(2);
    when(definitions.getIndexOf("start")).thenReturn(0);
    when(definitions.getIndexOf("end")).thenReturn(1);
    when(ps.getPreparedId()).thenReturn(newPreparedId(definitions, V4));
  }

  @Test
  public void should_create_record_mapper_when_mapping_keyspace_and_table_provided()
      throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_fail_to_create_schema_settings_when_mapping_many_to_one() throws Exception {
    LoaderConfig config =
        makeLoaderConfig("mapping = \"{ 0 = f1, 1 = f1}\", keyspace=ks, table=t1");

    @SuppressWarnings("ThrowableNotThrown")
    Throwable thrown = catchThrowable(() -> new SchemaSettings(config));
    assertThat(thrown)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Multiple input values in mapping resolve to column f1");
  }

  @Test
  public void should_create_record_mapper_when_mapping_ttl_and_timestamp() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format(
                    "mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s, "
                        + "1=__query_ttl, 3=__query_timestamp }\", ",
                    C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
                C1, C2, TTL_VARNAME, TIMESTAMP_VARNAME));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1,
        "1",
        TTL_VARNAME,
        "3",
        TIMESTAMP_VARNAME);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_mapping_function() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ now() = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s,\"%2$s\") VALUES (:%1$s,now())", C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "now()", C2, "2", C1);
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_with_static_ttl() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTtl=30");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TTL :%3$s",
                C1, C2, TTL_VARNAME));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(recordMapper, "ttl")).isEqualTo(30);
  }

  @Test
  public void should_create_record_mapper_with_static_timestamp() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=30");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TIMESTAMP :%3$s",
                C1, C2, TIMESTAMP_VARNAME));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(recordMapper, "timestamp")).isEqualTo(30L);
  }

  @Test
  public void should_create_record_mapper_with_static_timestamp_datetime() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=\"2017-01-02T00:00:01\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TIMESTAMP :%3$s",
                C1, C2, TIMESTAMP_VARNAME));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(recordMapper, "timestamp")).isEqualTo(1483315201000000L);
  }

  @Test
  public void should_create_record_mapper_with_static_timestamp_and_ttl() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=30, queryTtl=25");
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
                C1, C2, TTL_VARNAME, TIMESTAMP_VARNAME));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(recordMapper, "ttl")).isEqualTo(25);
    assertThat(Whitebox.getInternalState(recordMapper, "timestamp")).isEqualTo(30L);
  }

  @Test
  public void should_create_record_mapper_when_using_custom_query() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            "mapping = \"{ 0 = c1var , 2 = c2var }\", "
                + "query = \"INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)\", "
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)");
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"),
        "0",
        "c1var",
        "2",
        "c2var");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_mapping_is_a_list() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s)", C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "1", C1);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_mapping_and_statement_provided() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + String.format(
                    "query=\"insert into ks.table (%1$s,\\\"%2$s\\\") values (:%1$s,:\\\"%2$s\\\")\"",
                    C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    System.out.println(argument.getValue());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.table (%1$s,\"%2$s\") values (:%1$s,:\"%2$s\")", C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), "0", C2, "2", C1);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_keyspace_and_table_provided() throws Exception {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
    assertMapping((DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_with_inferred_mapping_and_override() throws Exception {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=*, %1$s = %2$s }\"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_with_infered_mapping_and_skip() throws Exception {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=\\\"-%1$s\\\" }\"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s,%2$s) VALUES (:%1$s,:%2$s)", C1, C3));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), C1, C1, C3, C3);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_with_infered_mapping_and_skip_multiple()
      throws Exception {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=[\\\"-%1$s\\\", -%2$s] }\"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s) VALUES (:%1$s)", C1));
    assertMapping((DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"), C1, C1);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_null_to_unset_is_false() throws Exception {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
    assertMapping((DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    assertThat((Set) Whitebox.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_null_words_are_provided() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = false, " + "nullStrings = \"NIL, NULL\", " + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
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
    assertMapping((DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping"));
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    Set<String> nullStrings = (Set<String>) Whitebox.getInternalState(recordMapper, NULL_STRINGS);
    assertThat(nullStrings).containsOnly("NIL", "NULL");
  }

  @Test
  public void should_create_settings_when_null_strings_are_specified() throws Exception {
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"null\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("null");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"null, NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("null", "NULL");
    }

    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"[NIL, NULL]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"\\\"NIL\\\", \\\"NULL\\\"\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"NIL, NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = NULL, keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"[NULL]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"[\\\"NULL\\\"]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);

      assertThat((Set<String>) Whitebox.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
  }

  @Test
  public void should_create_row_mapper_when_mapping_keyspace_and_table_provided() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\",%1$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_mapping_is_a_list() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT \"%2$s\",%1$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"), "0", C2, "1", C1);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_with_inferred_mapping_and_override() throws Exception {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=*, %1$s = %2$s }\"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2, C3));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_with_infered_mapping_and_skip() throws Exception {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=\\\"-%1$s\\\" }\"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,%2$s FROM ks.t1 WHERE token() > :start AND token() <= :end", C1, C3));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"), C1, C1, C3, C3);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_with_infered_mapping_and_skip_multiple() throws Exception {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=[\\\"-%1$s\\\", -%2$s] }\"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("SELECT %1$s FROM ks.t1 WHERE token() > :start AND token() <= :end", C1));
    assertMapping((DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"), C1, C1);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_mapping_and_statement_provided() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + String.format("query=\"select \\\"%2$s\\\",%1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("select \"%2$s\",%1$s from ks.t1", C1, C2));
    assertMapping(
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"), "0", C2, "2", C1);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_keyspace_and_table_provided() throws Exception {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"));
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_null_to_unset_is_false() throws Exception {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"));
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_null_words_are_provided() throws Exception {
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = false, " + "nullStrings = \"NIL, NULL\", " + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "SELECT %1$s,\"%2$s\",%3$s FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2, C3));
    assertMapping((DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping"));
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isEqualTo("NIL");
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
    Map<String, String> expected = new HashMap<>();
    for (int i = 0; i < fieldsAndVars.length; i += 2) {
      String first = fieldsAndVars[i];
      String second = fieldsAndVars[i + 1];
      expected.put(first, second);
    }
    Map<String, String> fieldsToVariables =
        (Map<String, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).isEqualTo(expected);
  }
}
