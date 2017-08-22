/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

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
import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.connectors.api.internal.SchemaFreeRecordMetadata;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.loader.engine.internal.schema.DefaultMapping;
import com.datastax.loader.engine.internal.schema.ReadResultMapper;
import com.datastax.loader.engine.internal.schema.RecordMapper;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class SchemaSettingsTest {

  private Session session;
  private ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private RecordMetadata recordMetadata = new SchemaFreeRecordMetadata();

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
    when(col1.getName()).thenReturn("c1");
    when(col2.getName()).thenReturn("c2");
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
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "mapping = { 0 = c2 , 2 = c1 }, "
                        + "nullToUnset = true, "
                        + "nullWords = [], "
                        + "keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c2,c1) VALUES (:c2,:c1)");
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo("c2");
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo("c1");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("0", "2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, "nullToUnset")).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, "nullWords")).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_mapping_and_statement_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "mapping = { 0 = c2 , 2 = c1 }, "
                        + "nullToUnset = true, "
                        + "nullWords = [], "
                        + "statement=\"insert into ks.table (c1,c2) values (:c1,:c2)\"")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("insert into ks.table (c1,c2) values (:c1,:c2)");
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo("c2");
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo("c1");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("0", "2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, "nullToUnset")).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, "nullWords")).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_keyspace_and_table_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("nullToUnset = true, nullWords = [], keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c1,c2) VALUES (:c1,:c2)");
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, "nullToUnset")).isTrue();
    assertThat((Set) Whitebox.getInternalState(recordMapper, "nullWords")).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_null_to_unset_is_false() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("nullToUnset = false, nullWords = [], keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c1,c2) VALUES (:c1,:c2)");
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, "nullToUnset")).isFalse();
    assertThat((Set) Whitebox.getInternalState(recordMapper, "nullWords")).isEmpty();
  }

  @Test
  public void should_create_record_mapper_when_null_words_are_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "nullToUnset = false, "
                        + "nullWords = [\"NIL\", \"NULL\"], "
                        + "keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("INSERT INTO ks.t1(c1,c2) VALUES (:c1,:c2)");
    DefaultMapping mapping = (DefaultMapping) Whitebox.getInternalState(recordMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat((Boolean) Whitebox.getInternalState(recordMapper, "nullToUnset")).isFalse();
    //noinspection unchecked
    assertThat((Set<String>) Whitebox.getInternalState(recordMapper, "nullWords"))
        .containsOnly("NIL", "NULL");
  }

  @Test
  public void should_create_row_mapper_when_mapping_keyspace_and_table_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "mapping = { 0 = c2 , 2 = c1 }, "
                        + "nullToUnset = true, "
                        + "nullWords = [], "
                        + "keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c2,c1 FROM ks.t1 WHERE token() > :start AND token() <= :end");
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(mapping.fieldToVariable("0")).isEqualTo("c2");
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo("c1");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("0", "2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_mapping_and_statement_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "mapping = { 0 = c2 , 2 = c1 }, "
                        + "nullToUnset = true, "
                        + "nullWords = [], "
                        + "statement=\"select c2,c1 from ks.t1\"")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue()).isEqualTo("select c2,c1 from ks.t1");
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable("0")).isEqualTo("c2");
    assertThat(mapping.fieldToVariable("1")).isNull();
    assertThat(mapping.fieldToVariable("2")).isEqualTo("c1");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("0", "2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_keyspace_and_table_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("nullToUnset = true, nullWords = [], keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1,c2 FROM ks.t1 WHERE token() > :start AND token() <= :end");
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_null_to_unset_is_false() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("nullToUnset = false, nullWords = [], keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1,c2 FROM ks.t1 WHERE token() > :start AND token() <= :end");
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  public void should_create_row_mapper_when_null_words_are_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "nullToUnset = false, "
                        + "nullWords = [\"NIL\", \"NULL\"], "
                        + "keyspace=ks, table=t1")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.schema")));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo("SELECT c1,c2 FROM ks.t1 WHERE token() > :start AND token() <= :end");
    DefaultMapping mapping =
        (DefaultMapping) Whitebox.getInternalState(readResultMapper, "mapping");
    assertThat(mapping.fieldToVariable("c1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("c2")).isEqualTo("c2");
    //noinspection unchecked
    Map<Object, String> fieldsToVariables =
        (Map<Object, String>) Whitebox.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables)
        .containsOnlyKeys("c1", "c2")
        .containsValue("c1")
        .containsValue("c2");
    assertThat(Whitebox.getInternalState(readResultMapper, "nullWord")).isEqualTo("NIL");
  }
}
