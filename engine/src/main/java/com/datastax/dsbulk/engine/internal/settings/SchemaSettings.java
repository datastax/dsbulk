/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.MergedRecordMetadata;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.executor.api.statement.TableScanner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaSettings implements SettingsValidator {

  private final LoaderConfig config;
  private final String INFERRED_MAPPING_TOKEN = "__INFERRED_MAPPING";

  private TableMetadata table;
  private String keyspaceName;
  private String tableName;
  private PreparedStatement preparedStatement;
  private ImmutableSet<String> nullStrings;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
    nullStrings = ImmutableSet.copyOf(config.getStringList("nullStrings"));
  }

  public RecordMapper createRecordMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    ImmutableBiMap<String, String> fieldsToVariables = createFieldsToVariablesMap(session);
    PreparedStatement statement = prepareStatement(session, fieldsToVariables, WorkflowType.LOAD);
    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, codecRegistry);
    return new DefaultRecordMapper(
        statement,
        mapping,
        mergeRecordMetadata(recordMetadata),
        nullStrings,
        config.getBoolean("nullToUnset"));
  }

  public ReadResultMapper createReadResultMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    ImmutableBiMap<String, String> fieldsToVariables = createFieldsToVariablesMap(session);
    preparedStatement = prepareStatement(session, fieldsToVariables, WorkflowType.UNLOAD);
    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, codecRegistry);
    return new DefaultReadResultMapper(
        mapping,
        mergeRecordMetadata(recordMetadata),
        nullStrings.isEmpty() ? null : nullStrings.iterator().next());
  }

  public List<Statement> createReadStatements(Cluster cluster) {
    ColumnDefinitions variables = preparedStatement.getVariables();
    if (variables.size() == 0) {
      return Collections.singletonList(preparedStatement.bind());
    }
    assert variables.size() == 2
            && variables.getIndexOf("start") != -1
            && variables.getIndexOf("end") != -1
        : "The provided statement contains unrecognized bound variables; only 'start' and 'end' can be used";
    Set<TokenRange> ring = cluster.getMetadata().getTokenRanges();
    return TableScanner.scan(
        ring,
        range ->
            preparedStatement
                .bind()
                .setToken("start", range.getStart())
                .setToken("end", range.getEnd()));
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getBoolean("nullToUnset");
      config.getStringList("nullStrings");
      boolean keyspaceTablePresent = false;
      if (config.hasPath("keyspace") && config.hasPath("table")) {
        keyspaceTablePresent = true;
      }

      // If table is present, keyspace must be, but not necessarily the other way around.
      if (config.hasPath("table") && !config.hasPath("keyspace")) {
        throw new BulkConfigurationException(
            "schema.keyspace must accompany schema.table in the configuration", "schema");
      }

      // If mapping is present, make sure it is parseable as a map.
      if (config.hasPath("mapping")) {
        Config mapping = getMapping();
        if (mapping.hasPath(INFERRED_MAPPING_TOKEN) && !keyspaceTablePresent) {
          throw new BulkConfigurationException(
              "schema.keyspace and schema.table must be defined when using inferred mapping",
              "schema");
        }
      }

      // Either the keyspace and table must be present, or the mapping must be present.
      if (!config.hasPath("mapping") && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            "schema.mapping, or schema.keyspace and schema.table must be defined", "schema");
      }

      // Either the keyspace and table must be present, or the mapping must be present.
      if (!config.hasPath("query") && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            "schema.query, or schema.keyspace and schema.table must be defined", "schema");
      }

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "schema");
    }
  }

  public String getKeyspace() {
    String keyspace = config.getString("keyspace");
    if (keyspace != null && keyspace.isEmpty()) {
      keyspace = null;
    }
    if (keyspace != null) {
      keyspace = Metadata.quoteIfNecessary(keyspace);
    }
    return keyspace;
  }

  private ImmutableBiMap<String, String> createFieldsToVariablesMap(Session session)
      throws BulkConfigurationException {
    BiMap<String, String> fieldsToVariables = null;
    BiMap<String, String> explicitVariables = HashBiMap.create();

    if (config.hasPath("keyspace") && config.hasPath("table")) {
      keyspaceName = Metadata.quoteIfNecessary(config.getString("keyspace"));
      tableName = Metadata.quoteIfNecessary(config.getString("table"));
      KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      Preconditions.checkNotNull(keyspace, "Keyspace does not exist: " + keyspaceName);
      table = keyspace.getTable(tableName);
      Preconditions.checkNotNull(
          table, String.format("Table does not exist: %s.%s", keyspaceName, tableName));
    }

    if (!config.hasPath("mapping")) {
      fieldsToVariables = inferFieldsToVariablesMap();
    } else {
      Config mapping = getMapping();
      if (mapping.hasPath(INFERRED_MAPPING_TOKEN)) {
        fieldsToVariables =
            inferFieldsToVariablesMap(
                new InferredMappingSpec(mapping.getValue(INFERRED_MAPPING_TOKEN)));
      }
      if (fieldsToVariables == null) {
        fieldsToVariables = HashBiMap.create();
      }
      for (String path : mapping.withoutPath(INFERRED_MAPPING_TOKEN).root().keySet()) {
        if (explicitVariables.containsValue(mapping.getString(path))) {
          if (mapping.getString(path).equals(explicitVariables.get(path))) {
            // This mapping already exists. Skip it.
            continue;
          }
          throw new BulkConfigurationException(
              "Multiple input values in mapping resolve to column "
                  + mapping.getString(path)
                  + ". "
                  + "Please review schema.mapping for duplicates.",
              "schema.mapping");
        }
        explicitVariables.forcePut(path, mapping.getString(path));
      }
      for (Map.Entry<String, String> entry : explicitVariables.entrySet()) {
        fieldsToVariables.forcePut(entry.getKey(), entry.getValue());
      }
    }
    validateAllFieldsPresent(fieldsToVariables);
    validateAllKeysPresent(fieldsToVariables);
    Preconditions.checkNotNull(
        fieldsToVariables,
        "Mapping was absent and could not be inferred, please provide an explicit mapping");

    return ImmutableBiMap.copyOf(fieldsToVariables);
  }

  private void validateAllFieldsPresent(BiMap<String, String> fieldsToVariables) {
    if (table != null) {
      fieldsToVariables.forEach(
          (key, value) -> {
            if (table.getColumn(value) == null) {
              throw new BulkConfigurationException(
                  "Schema mapping "
                      + value
                      + " doesn't match any column found in table "
                      + table.getName(),
                  "schema.mapping");
            }
          });
    }
  }

  private void validateAllKeysPresent(BiMap<String, String> fieldsToVariables) {
    if (table != null) {
      List<ColumnMetadata> primaryKeys = table.getPrimaryKey();
      primaryKeys.forEach(
          key -> {
            if (!fieldsToVariables.containsValue(key.getName())) {
              throw new BulkConfigurationException(
                  "Missing required key column of "
                      + key.getName()
                      + " from header or schema.mapping. Please ensure it's included in the header or mapping",
                  "schema.mapping");
            }
          });
    }
  }

  private Config getMapping() throws BulkConfigurationException {
    String mappingString = config.getString("mapping").replaceAll("\\*", INFERRED_MAPPING_TOKEN);
    try {
      return ConfigFactory.parseString(mappingString);
    } catch (ConfigException.Parse e) {
      // mappingString doesn't seem to be a map. Treat it as a list instead.
      Map<String, String> indexMap = new HashMap<>();
      int curInd = 0;
      for (String s : config.getStringList("mapping")) {
        indexMap.put(Integer.toString(curInd++), s);
      }
      return ConfigFactory.parseMap(indexMap);
    }
  }

  private RecordMetadata mergeRecordMetadata(RecordMetadata fallback) {
    if (config.hasPath("recordMetadata")) {
      ImmutableMap.Builder<String, TypeToken<?>> fieldsToTypes = new ImmutableMap.Builder<>();
      LoaderConfig recordMetadata =
          new DefaultLoaderConfig(ConfigFactory.parseString(config.getString("recordMetadata")));
      for (String path : recordMetadata.root().keySet()) {
        fieldsToTypes.put(path, TypeToken.of(recordMetadata.getClass(path)));
      }
      return new MergedRecordMetadata(fieldsToTypes.build(), fallback);
    }
    return fallback;
  }

  private PreparedStatement prepareStatement(
      Session session,
      ImmutableBiMap<String, String> fieldsToVariables,
      WorkflowType workflowType) {
    String query;
    if (config.hasPath("query")) {
      query = config.getString("query");
    } else {
      query =
          workflowType == WorkflowType.LOAD
              ? inferWriteQuery(fieldsToVariables)
              : inferReadQuery(fieldsToVariables);
    }
    return session.prepare(query);
  }

  private BiMap<String, String> inferFieldsToVariablesMap() {
    return inferFieldsToVariablesMap(null);
  }

  private BiMap<String, String> inferFieldsToVariablesMap(InferredMappingSpec spec) {
    HashBiMap<String, String> fieldsToVariables = HashBiMap.create();
    for (int i = 0; i < table.getColumns().size(); i++) {
      ColumnMetadata col = table.getColumns().get(i);
      if (spec == null || spec.allow(col.getName())) {
        // don't quote column names here, it will be done later on if required
        fieldsToVariables.put(col.getName(), col.getName());
      }
    }
    return fieldsToVariables;
  }

  private String inferWriteQuery(ImmutableBiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    appendColumnNames(fieldsToVariables, sb);
    sb.append(") VALUES (");
    Set<String> cols = new LinkedHashSet<>(fieldsToVariables.values());
    Iterator<String> it = cols.iterator();
    while (it.hasNext()) {
      String col = it.next();
      sb.append(':');
      sb.append(Metadata.quoteIfNecessary(col));
      if (it.hasNext()) {
        sb.append(',');
      }
    }
    sb.append(')');
    return sb.toString();
  }

  private String inferReadQuery(ImmutableBiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("SELECT ");
    appendColumnNames(fieldsToVariables, sb);
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName).append(" WHERE ");
    appendTokenFunction(sb, table.getPartitionKey());
    sb.append(" > :start AND ");
    appendTokenFunction(sb, table.getPartitionKey());
    sb.append(" <= :end");
    return sb.toString();
  }

  private static void appendColumnNames(
      ImmutableBiMap<String, String> fieldsToVariables, StringBuilder sb) {
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<String> cols = new LinkedHashSet<>(fieldsToVariables.values());
    Iterator<String> it = cols.iterator();
    while (it.hasNext()) {
      // this assumes that the variable name found in the mapping
      // corresponds to a CQL column having the exact same name.
      String col = it.next();
      sb.append(Metadata.quoteIfNecessary(col));
      if (it.hasNext()) {
        sb.append(',');
      }
    }
  }

  private static void appendTokenFunction(StringBuilder sb, Iterable<ColumnMetadata> partitionKey) {
    sb.append("token(");
    Iterator<ColumnMetadata> pks = partitionKey.iterator();
    while (pks.hasNext()) {
      ColumnMetadata pk = pks.next();
      sb.append(Metadata.quoteIfNecessary(pk.getName()));
      if (pks.hasNext()) {
        sb.append(',');
      }
    }
    sb.append(')');
  }

  private class InferredMappingSpec {
    private Set<String> excludes = new HashSet<>();

    InferredMappingSpec(ConfigValue spec) {
      if (spec.valueType() == ConfigValueType.STRING) {
        processSpec((String) spec.unwrapped());
      } else if (spec.valueType() == ConfigValueType.LIST) {
        @SuppressWarnings("unchecked")
        List<Object> specList = (List<Object>) spec.unwrapped();
        specList.forEach(x -> processSpec((String) x));
      }
    }

    private void processSpec(String specString) {
      if (specString.startsWith("-")) {
        // We're excluding a particular column. This implies that
        // we include all others.
        excludes.add(specString.substring(1));
      }
    }

    boolean allow(String name) {
      return !excludes.contains(name);
    }
  }
}
