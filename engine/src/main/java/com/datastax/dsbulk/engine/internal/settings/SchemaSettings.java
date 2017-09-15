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
import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** */
public class SchemaSettings {

  private final LoaderConfig config;

  private KeyspaceMetadata keyspace;
  private TableMetadata table;
  private String keyspaceName;
  private String tableName;
  private PreparedStatement preparedStatement;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public RecordMapper createRecordMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry) {
    ImmutableBiMap<String, String> fieldsToVariables = createFieldsToVariablesMap(session);
    PreparedStatement statement = prepareStatement(session, fieldsToVariables, WorkflowType.LOAD);
    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, codecRegistry);
    return new DefaultRecordMapper(
        statement,
        mapping,
        mergeRecordMetadata(recordMetadata),
        ImmutableSet.copyOf(config.getStringList("nullStrings")),
        config.getBoolean("nullToUnset"));
  }

  public ReadResultMapper createReadResultMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry) {
    ImmutableBiMap<String, String> fieldsToVariables = createFieldsToVariablesMap(session);
    preparedStatement = prepareStatement(session, fieldsToVariables, WorkflowType.UNLOAD);
    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, codecRegistry);
    return new DefaultReadResultMapper(
        mapping, mergeRecordMetadata(recordMetadata), config.getFirstString("nullStrings"));
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

  private ImmutableBiMap<String, String> createFieldsToVariablesMap(Session session) {
    ImmutableBiMap.Builder<String, String> fieldsToVariablesBuilder = null;
    if (config.hasPath("mapping")) {
      fieldsToVariablesBuilder = new ImmutableBiMap.Builder<>();
      Config mapping = ConfigFactory.parseString(config.getString("mapping"));
      for (String path : mapping.root().keySet()) {
        fieldsToVariablesBuilder.put(path, mapping.getString(path));
      }
    }
    if (config.hasPath("keyspace")) {
      Preconditions.checkState(config.hasPath("table"), "Keyspace and table must be specified");
      keyspaceName = Metadata.quoteIfNecessary(config.getString("keyspace"));
      tableName = Metadata.quoteIfNecessary(config.getString("table"));
      keyspace = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      Preconditions.checkNotNull(keyspace, "Keyspace does not exist: " + keyspaceName);
      table = keyspace.getTable(tableName);
      Preconditions.checkNotNull(
          table, String.format("Table does not exist: %s.%s", keyspaceName, tableName));
    }
    if (!config.hasPath("mapping")) {
      Preconditions.checkState(
          keyspace != null && table != null, "Keyspace and table must be specified");
      fieldsToVariablesBuilder = inferFieldsToVariablesMap();
    }
    Preconditions.checkNotNull(
        fieldsToVariablesBuilder,
        "Mapping was absent and could not be inferred, please provide an explicit mapping");
    return fieldsToVariablesBuilder.build();
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
      Preconditions.checkState(
          keyspace != null && table != null, "Keyspace and table must be specified");
      query =
          workflowType == WorkflowType.LOAD
              ? inferWriteQuery(fieldsToVariables)
              : inferReadQuery(fieldsToVariables);
    }
    return session.prepare(query);
  }

  private ImmutableBiMap.Builder<String, String> inferFieldsToVariablesMap() {
    ImmutableBiMap.Builder<String, String> fieldsToVariables = new ImmutableBiMap.Builder<>();
    for (int i = 0; i < table.getColumns().size(); i++) {
      ColumnMetadata col = table.getColumns().get(i);
      // don't quote column names here, it will be done later on if required
      fieldsToVariables.put(col.getName(), col.getName());
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
}
