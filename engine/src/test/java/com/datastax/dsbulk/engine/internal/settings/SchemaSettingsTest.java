/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newPreparedId;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.SchemaFreeRecordMetadata;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

  private static final String NULL_STRINGS = "nullStrings";
  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";
  private static final String C4 = "c4";
  private static final String TTL_VARNAME =
      (String) ReflectionUtils.getInternalState(SchemaSettings.class, "TTL_VARNAME");
  private static final String TIMESTAMP_VARNAME =
      (String) ReflectionUtils.getInternalState(SchemaSettings.class, "TIMESTAMP_VARNAME");

  private Session session;
  private PreparedStatement ps;

  private final ExtendedCodecRegistry codecRegistry = mock(ExtendedCodecRegistry.class);
  private final RecordMetadata recordMetadata = new SchemaFreeRecordMetadata();
  private ThreadLocal<DecimalFormat> numberFormat =
      ThreadLocal.withInitial(
          () -> {
            DecimalFormat format = new DecimalFormat("#,###.##");
            format.setParseBigDecimal(true);
            return format;
          });
  private final StringToInstantCodec codec =
      new StringToInstantCodec(CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));

  @BeforeEach
  void setUp() {
    session = mock(Session.class);
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    TableMetadata table = mock(TableMetadata.class);
    ps = mock(PreparedStatement.class);
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
    ColumnDefinitions definitions =
        newColumnDefinitions(
            newDefinition(C1, varchar()),
            newDefinition(C2, varchar()),
            newDefinition(C3, varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    when(ps.getPreparedId()).thenReturn(newPreparedId(definitions, V4));
  }

  @Test
  void should_create_record_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_fail_to_create_schema_settings_when_mapping_many_to_one() {
    LoaderConfig config =
        makeLoaderConfig("mapping = \"{ 0 = f1, 1 = f1}\", keyspace=ks, table=t1");
    assertThrows(
        BulkConfigurationException.class,
        () -> {
          SchemaSettings schemaSettings = new SchemaSettings(config);
          schemaSettings.init(codec);
        },
        "Multiple input values in mapping resolve to column f1");
  }

  @Test
  void should_create_record_mapper_when_mapping_ttl_and_timestamp() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format(
                    "mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s, 1=__ttl, 3=__timestamp }\", ",
                    C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1,
        "1",
        TTL_VARNAME,
        "3",
        TIMESTAMP_VARNAME);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_mapping_function() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ now() = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(String.format("INSERT INTO ks.t1(%1$s,\"%2$s\") VALUES (:%1$s,now())", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"), "2", C1);
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_with_static_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTtl=30");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=123456789");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TIMESTAMP 123456789000",
                C1, C2));
  }

  @Test
  void should_create_record_mapper_with_static_timestamp_datetime() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=\"2017-01-02T00:00:01\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
  void should_create_record_mapper_with_static_timestamp_datetime_custom() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=\"20171123102034\"");
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("Europe/Paris"));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(
        new StringToInstantCodec(formatter, numberFormat, MILLISECONDS, EPOCH.atZone(UTC)));
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) USING TIMESTAMP %3$s",
                C1,
                C2,
                instantToNumber(
                    Instant.from(formatter.parse("20171123102034")), MICROSECONDS, EPOCH)));
  }

  @Test
  void should_create_record_mapper_with_static_timestamp_and_ttl() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "keyspace=ks, table=t1, queryTimestamp=123456789, queryTtl=25");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format(
                "INSERT INTO ks.t1(\"%2$s\",%1$s) VALUES (:\"%2$s\",:%1$s) "
                    + "USING TTL 25 AND TIMESTAMP 123456789000",
                C1, C2));
  }

  @Test
  void should_create_record_mapper_when_using_custom_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(newDefinition("c1var", varchar()), newDefinition("c2var", varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "mapping = \"{ 0 = c1var , 2 = c2var }\", "
                + "query = \"INSERT INTO ks.t1(c2, c1) VALUES (:c2var, :c1var)\", "
                + "nullToUnset = true");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_mapping_is_a_list() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_mapping_and_statement_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + String.format(
                    "query=\"insert into ks.table (%1$s,\\\"%2$s\\\") values (:%1$s,:\\\"%2$s\\\")\"",
                    C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    assertThat(recordMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("insert into ks.table (%1$s,\"%2$s\") values (:%1$s,:\"%2$s\")", C1, C2));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=*, %1$s = %2$s }\"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(recordMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
    assertThat((Boolean) ReflectionUtils.getInternalState(recordMapper, NULL_TO_UNSET)).isTrue();
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_with_inferred_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=\\\"-%1$s\\\" }\"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_with_infered_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=[\\\"-%1$s\\\", -%2$s] }\"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat((Set) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS)).isEmpty();
  }

  @Test
  void should_create_record_mapper_when_null_words_are_provided() {
    LoaderConfig config =
        makeLoaderConfig("nullToUnset = false, nullStrings = \"NIL, NULL\", keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    Set<String> nullStrings =
        (Set<String>) ReflectionUtils.getInternalState(recordMapper, NULL_STRINGS);
    assertThat(nullStrings).containsOnly("NIL", "NULL");
  }

  @Test
  void should_create_settings_when_null_strings_are_specified() {
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"null\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("null");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"null, NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("null", "NULL");
    }

    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"[NIL, NULL]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"\\\"NIL\\\", \\\"NULL\\\"\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"NIL, NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NIL", "NULL");
    }

    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"NULL\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = NULL, keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = \"[NULL]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config =
          makeLoaderConfig("nullStrings = \"[\\\"NULL\\\"]\", keyspace=ks, table=t1");
      SchemaSettings schemaSettings = new SchemaSettings(config);
      schemaSettings.init(codec);

      assertThat((Set<String>) ReflectionUtils.getInternalState(schemaSettings, NULL_STRINGS))
          .containsOnly("NULL");
    }
  }

  @Test
  void should_create_row_mapper_when_mapping_keyspace_and_table_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        "0",
        C2,
        "2",
        C1);
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_when_mapping_is_a_list() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"\\\"%2$s\\\", %1$s\", ", C1, C2)
                + "nullToUnset = true, "
                + "keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        "0",
        C2,
        "1",
        C1);
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_with_inferred_mapping_and_override() {
    // Infer mapping, but override to set c4 source field to C3 column.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=*, %1$s = %2$s }\"", C4, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C2,
        C2,
        C4,
        C3);
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_with_infered_mapping_and_skip() {
    // Infer mapping, but skip C2.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=\\\"-%1$s\\\" }\"", C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"),
        C1,
        C1,
        C3,
        C3);
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_with_infered_mapping_and_skip_multiple() {
    // Infer mapping, but skip C2 and C3.
    LoaderConfig config =
        makeLoaderConfig(
            "nullToUnset = true, keyspace=ks, table=t1, "
                + String.format("mapping = \"{ *=[\\\"-%1$s\\\", -%2$s] }\"", C2, C3));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    ReadResultMapper readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    assertThat(readResultMapper).isNotNull();
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(session).prepare(argument.capture());
    assertThat(argument.getValue())
        .isEqualTo(
            String.format("SELECT %1$s FROM ks.t1 WHERE token() > :start AND token() <= :end", C1));
    assertMapping(
        (DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"), C1, C1);
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_when_mapping_and_statement_provided() {
    LoaderConfig config =
        makeLoaderConfig(
            String.format("mapping = \"{ 0 = \\\"%2$s\\\" , 2 = %1$s }\", ", C1, C2)
                + "nullToUnset = true, "
                + String.format("query=\"select \\\"%2$s\\\",%1$s from ks.t1\"", C1, C2));
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_when_keyspace_and_table_provided() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = true, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"));
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_when_null_to_unset_is_false() {
    LoaderConfig config = makeLoaderConfig("nullToUnset = false, keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"));
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isNull();
  }

  @Test
  void should_create_row_mapper_when_null_words_are_provided() {
    LoaderConfig config =
        makeLoaderConfig("nullToUnset = false, nullStrings = \"NIL, NULL\", keyspace=ks, table=t1");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
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
    assertMapping((DefaultMapping) ReflectionUtils.getInternalState(readResultMapper, "mapping"));
    assertThat(ReflectionUtils.getInternalState(readResultMapper, "nullWord")).isEqualTo("NIL");
  }

  @Test
  void should_use_default_writetime_var_name() {
    LoaderConfig config =
        makeLoaderConfig("keyspace = ks, table = t1, mapping = \"{ *=*, f1 = __timestamp }\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable"))
        .isEqualTo(TIMESTAMP_VARNAME);
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
                + "mapping = \"{ f1 = c1 , f2 = c2 , f3 = c3 }\" ");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable")).isEqualTo("c3");
  }

  @Test
  void should_detect_quoted_writetime_var_in_query() {
    ColumnDefinitions definitions =
        newColumnDefinitions(
            newDefinition("k", varchar()),
            newDefinition("c", varchar()),
            newDefinition("\"This is a quoted \\\" variable name\"", varchar()));
    when(ps.getVariables()).thenReturn(definitions);
    LoaderConfig config =
        makeLoaderConfig(
            "query = \"INSERT INTO ks.t1 (k,c) VALUES (:k,:c) USING TTL 123 AND tImEsTaMp     :\\\"This is a quoted \\\"\\\" variable name\\\"\"");
    SchemaSettings schemaSettings = new SchemaSettings(config);
    schemaSettings.init(codec);
    RecordMapper mapper = schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);
    DefaultMapping mapping = (DefaultMapping) ReflectionUtils.getInternalState(mapper, "mapping");
    assertThat(mapping).isNotNull();
    assertThat(ReflectionUtils.getInternalState(mapping, "writeTimeVariable"))
        .isEqualTo("This is a quoted \" variable name");
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
        (Map<String, String>) ReflectionUtils.getInternalState(mapping, "fieldsToVariables");
    assertThat(fieldsToVariables).isEqualTo(expected);
  }
}
