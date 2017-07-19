/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.loader.engine.internal.schema.DefaultRecordMapper;
import com.datastax.loader.engine.schema.RecordMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

  public SchemaSettings(Config config) {
    this.config = config;
  }

  public RecordMapper newRecordMapper(Session session) {
    Map<String, String> mapping = null;
    if (config.hasPath("mapping")) {
      mapping = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry :
          config.getConfig("mapping").root().unwrapped().entrySet()) {
        mapping.put(entry.getKey(), entry.getValue().toString());
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
      query = inferQuery(new LinkedHashSet<>(mapping.values()));
    }
    PreparedStatement ps = session.prepare(query);
    return new DefaultRecordMapper(ps, mapping);
  }

  private Map<String, String> inferMapping() {
    Map<String, String> mapping = new LinkedHashMap<>();
    for (int i = 0; i < table.getColumns().size(); i++) {
      ColumnMetadata col = table.getColumns().get(i);
      String name = Metadata.quoteIfNecessary(col.getName());
      // use both indexed and mapped access
      // indexed access can only work if the data source produces
      // indexed records with columns in the same order
      mapping.put(Integer.toString(i), name);
      mapping.put(col.getName(), name);
    }
    return mapping;
  }

  private String inferQuery(Set<String> mappedCols) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    Iterator<String> it = mappedCols.iterator();
    while (it.hasNext()) {
      String col = it.next();
      sb.append(col);
      if (it.hasNext()) sb.append(',');
    }
    sb.append(") VALUES (");
    it = mappedCols.iterator();
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
