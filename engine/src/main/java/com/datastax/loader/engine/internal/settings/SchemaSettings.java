/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.loader.engine.internal.schema.DefaultMapping;
import com.datastax.loader.engine.internal.schema.RecordMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** */
public class SchemaSettings {

  private final LoaderConfig config;

  private KeyspaceMetadata keyspace;
  private TableMetadata table;
  private String keyspaceName;
  private String tableName;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public RecordMapper init(Session session, ExtendedCodecRegistry codecRegistry) {
    ImmutableMap.Builder<Object, String> fieldsToVariablesBuilder = null;
    if (config.hasPath("mapping") && !config.getConfig("mapping").isEmpty()) {
      fieldsToVariablesBuilder = new ImmutableMap.Builder<>();
      for (Map.Entry<String, Object> entry :
          config.getConfig("mapping").root().unwrapped().entrySet()) {
        Object key = entry.getKey();
        // Since the config library doesn't allow integer map keys,
        // parse them now if possible
        try {
          key = Integer.valueOf(key.toString());
        } catch (NumberFormatException ignored) {
        }
        fieldsToVariablesBuilder.put(key, entry.getValue().toString());
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
    if (!config.hasPath("mapping") || config.getConfig("mapping").isEmpty()) {
      Preconditions.checkState(
          keyspace != null && table != null, "Keyspace and table must be specified");
      fieldsToVariablesBuilder = inferFieldsToVariablesMap();
    }
    Preconditions.checkNotNull(
        fieldsToVariablesBuilder,
        "Mapping was absent and could not be inferred, please provide an explicit mapping");
    ImmutableMap<Object, String> fieldsToVariables = fieldsToVariablesBuilder.build();
    String query;
    if (config.hasPath("statement")) {
      query = config.getString("statement");
    } else {
      Preconditions.checkState(
          keyspace != null && table != null, "Keyspace and table must be specified");
      query = inferQuery(fieldsToVariables);
    }
    PreparedStatement ps = session.prepare(query);
    DefaultMapping mapping =
        new DefaultMapping(fieldsToVariables, codecRegistry, ps.getVariables());
    return new RecordMapper(
        ps, mapping, config.getStringList("nullWords"), config.getBoolean("nullToUnset"));
  }

  private ImmutableMap.Builder<Object, String> inferFieldsToVariablesMap() {
    ImmutableMap.Builder<Object, String> fieldsToVariables = new ImmutableMap.Builder<>();
    for (int i = 0; i < table.getColumns().size(); i++) {
      ColumnMetadata col = table.getColumns().get(i);
      String name = Metadata.quoteIfNecessary(col.getName());
      // use both indexed and mapped access
      // indexed access can only work if the data source produces
      // indexed records with columns in the same order
      fieldsToVariables.put(i, name);
      fieldsToVariables.put(col.getName(), name);
    }
    return fieldsToVariables;
  }

  private String inferQuery(ImmutableMap<Object, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<String> cols = new LinkedHashSet<>(fieldsToVariables.values());
    Iterator<String> it = cols.iterator();
    while (it.hasNext()) {
      // this assumes that the variable name found in the mapping
      // corresponds to a CQL column having the exact same name.
      String col = it.next();
      sb.append(Metadata.quoteIfNecessary(col));
      if (it.hasNext()) sb.append(',');
    }
    sb.append(") VALUES (");
    it = cols.iterator();
    while (it.hasNext()) {
      String col = it.next();
      sb.append(':');
      sb.append(col);
      if (it.hasNext()) sb.append(',');
    }
    sb.append(')');
    return sb.toString();
  }
}
