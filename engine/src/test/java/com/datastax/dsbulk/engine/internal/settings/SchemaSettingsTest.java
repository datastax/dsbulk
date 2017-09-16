/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DriverCoreTestHooks.newPreparedId;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
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
import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.SchemaFreeRecordMetadata;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
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
    List<ColumnMetadata> columns = Lists.newArrayList(col1, col2);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(keyspace);
    when(keyspace.getTable(anyString())).thenReturn(table);
    when(session.prepare(anyString())).thenReturn(ps);
    when(table.getColumns()).thenReturn(columns);
    when(col1.getName()).thenReturn(C1);
    when(col2.getName()).thenReturn(C2);
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
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo(C2);
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo(C1);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys("0", "2").containsValue(C1).containsValue(C2);
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
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.table (%1$s,\"%2$s\") values (:%1$s,:\"%2$s\")", C1, C2));
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo(C2);
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo(C1);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys("0", "2").containsValue(C1).containsValue(C2);
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
            String.format("INSERT INTO ks.t1(%1$s,\"%2$s\") VALUES (:%1$s,:\"%2$s\")", C1, C2));
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
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
            String.format("INSERT INTO ks.t1(%1$s,\"%2$s\") VALUES (:%1$s,:\"%2$s\")", C1, C2));
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
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
            String.format("INSERT INTO ks.t1(%1$s,\"%2$s\") VALUES (:%1$s,:\"%2$s\")", C1, C2));
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, NULL_TO_UNSET)).isFalse();
    Set<String> nullStrings = (Set<String>) Whitebox.getInternalState(recordMapper, NULL_STRINGS);
    assertThat(nullStrings).containsOnly("NIL", "NULL");
  }

  @Test
  public void should_create_settings_when_null_strings_are_specified() throws Exception {
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
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(mapping.fieldToVariable("0")).isEqualTo(C2);
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo(C1);
    assertThat(fieldsToVariables).containsOnlyKeys("0", "2").containsValue(C1).containsValue(C2);
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
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo(C2);
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo(C1);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys("0", "2").containsValue(C1).containsValue(C2);
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
                "SELECT %1$s,\"%2$s\" FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2));
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
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
                "SELECT %1$s,\"%2$s\" FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2));
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
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
                "SELECT %1$s,\"%2$s\" FROM ks.t1 WHERE token() > :start AND token() <= :end",
                C1, C2));
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable(C1)).isEqualTo(C1);
    assertThat(mapping.fieldToVariable(C2)).isEqualTo(C2);
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).containsOnlyKeys(C1, C2).containsValue(C1).containsValue(C2);
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isEqualTo("NIL");
  }

  @NotNull
  private static LoaderConfig makeLoaderConfig(String configString) {
    return new DefaultLoaderConfig(
        ConfigFactory.parseString(configString)
            .withFallback(ConfigFactory.load().getConfig("dsbulk.schema")));
  }
}
