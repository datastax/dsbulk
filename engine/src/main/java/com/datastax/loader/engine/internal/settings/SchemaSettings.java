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
import com.datastax.loader.engine.internal.schema.DefaultMapping;
import com.datastax.loader.engine.internal.schema.RecordMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** */
public class SchemaSettings {

  private final Config config;

  private KeyspaceMetadata keyspace;
  private TableMetadata table;
  private String keyspaceName;
  private String tableName;

  SchemaSettings(Config config) {
    this.config = config;
  }

  public RecordMapper newRecordMapper(Session session) {
    DefaultMapping mapping = null;
    if (config.hasPath("mapping")) {
      mapping = new DefaultMapping();
      for (Map.Entry<String, Object> entry :
          config.getConfig("mapping").root().unwrapped().entrySet()) {
        Object key = entry.getKey();
        // Since the config library doesn't allow integer map keys,
        // parse them now if possible
        try {
          key = Integer.valueOf(key.toString());
        } catch (NumberFormatException ignored) {
        }
        mapping.put(key, entry.getValue().toString());
      }
    }
    if (config.hasPath("keyspace")) {
      Preconditions.checkState(config.hasPath("table"));
      keyspaceName = Metadata.quoteIfNecessary(config.getString("keyspace"));
      tableName = Metadata.quoteIfNecessary(config.getString("table"));
      keyspace = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      Preconditions.checkNotNull(keyspace);
      table = keyspace.getTable(tableName);
      Preconditions.checkNotNull(table);
    }
    if (!config.hasPath("mapping")) {
      Preconditions.checkState(keyspace != null && table != null);
      mapping = inferMapping();
    }
    Preconditions.checkNotNull(mapping);
    String query;
    if (config.hasPath("statement")) {
      query = config.getString("statement");
    } else {
      Preconditions.checkState(keyspace != null && table != null);
      query = inferQuery(mapping);
    }
    PreparedStatement ps = session.prepare(query);
    return new RecordMapper(
        ps, mapping, config.getStringList("input-null-words"), config.getBoolean("null-to-unset"));
  }

  private DefaultMapping inferMapping() {
    DefaultMapping mapping = new DefaultMapping();
    for (int i = 0; i < table.getColumns().size(); i++) {
      ColumnMetadata col = table.getColumns().get(i);
      String name = Metadata.quoteIfNecessary(col.getName());
      // use both indexed and mapped access
      // indexed access can only work if the data source produces
      // indexed records with columns in the same order
      mapping.put(i, name);
      mapping.put(col.getName(), name);
    }
    return mapping;
  }

  private String inferQuery(DefaultMapping mapping) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<String> cols = new LinkedHashSet<>(mapping.values());
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
