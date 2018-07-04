/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.simulacron;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class SimulacronUtils {

  private static final String SELECT_KEYSPACES = "SELECT * FROM system_schema.keyspaces";
  private static final String SELECT_TABLES = "SELECT * FROM system_schema.tables";
  private static final String SELECT_COLUMNS = "SELECT * FROM system_schema.columns";

  private static final ImmutableMap<String, String> KEYSPACE_COLUMNS =
      ImmutableMap.of(
          "keyspace_name", "varchar",
          "durable_writes", "boolean",
          "replication", "map<varchar, varchar>");

  private static final ImmutableMap<String, String> TABLE_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("keyspace_name", "varchar")
          .put("table_name", "varchar")
          .put("bloom_filter_fp_chance", "double")
          .put("caching", "map<varchar, varchar>")
          .put("cdc", "boolean")
          .put("comment", "varchar")
          .put("compaction", "map<varchar, varchar>")
          .put("compression", "map<varchar, varchar>")
          .put("crc_check_chance", "double")
          .put("dclocal_read_repair_chance", "double")
          .put("default_time_to_live", "int")
          //          .put("extensions", "map<varchar, blob>")
          .put("flags", "set<varchar>")
          .put("gc_grace_seconds", "int")
          .put("id", "uuid")
          .put("max_index_interval", "int")
          .put("memtable_flush_period_in_ms", "int")
          .put("min_index_interval", "int")
          .put("read_repair_chance", "double")
          .put("speculative_retry", "varchar")
          .build();

  private static final ImmutableMap<String, String> COLUMN_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("keyspace_name", "varchar")
          .put("table_name", "varchar")
          .put("column_name", "varchar")
          .put("clustering_order", "varchar")
          .put("column_name_bytes", "blob")
          .put("kind", "varchar")
          .put("position", "int")
          .put("type", "varchar")
          .build();

  public static class Keyspace {

    private String name;
    private List<Table> tables;

    public Keyspace(String name, Table... tables) {
      this.name = name;
      this.tables = Arrays.asList(tables);
    }
  }

  public static class Table {

    private String name;
    private List<Column> partitionKey;
    private List<Column> clusteringColumns;
    private List<Column> otherColumns;
    private List<Map<String, Object>> rows;

    public Table(
        String name, Column partitionKey, Column clusteringColumn, Column... otherColumns) {
      this(
          name,
          Collections.singletonList(partitionKey),
          Collections.singletonList(clusteringColumn),
          Arrays.asList(otherColumns));
    }

    public Table(
        String name,
        List<Column> partitionKey,
        List<Column> clusteringColumns,
        List<Column> otherColumns) {
      this(name, partitionKey, clusteringColumns, otherColumns, new ArrayList<>());
    }

    public Table(
        String name,
        List<Column> partitionKey,
        List<Column> clusteringColumns,
        List<Column> otherColumns,
        List<Map<String, Object>> rows) {
      this.name = name;
      this.partitionKey = partitionKey;
      this.clusteringColumns = clusteringColumns;
      this.otherColumns = otherColumns;
      this.rows = rows;
    }

    private List<Column> allColumns() {
      List<Column> all = new ArrayList<>();
      all.addAll(partitionKey);
      all.addAll(clusteringColumns);
      all.addAll(otherColumns);
      return all;
    }

    private Map<String, String> allColumnTypes() {
      return allColumns()
          .stream()
          .map(col -> new SimpleEntry<>(col.name, col.type.toString()))
          .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }
  }

  public static class Column {

    private String name;
    private DataType type;

    public Column(String name, DataType type) {
      this.name = name;
      this.type = type;
    }
  }

  public static void primeTables(BoundCluster simulacron, Keyspace... keyspaces) {

    List<Map<String, Object>> allKeyspacesRows = new ArrayList<>();
    List<Map<String, Object>> allTablesRows = new ArrayList<>();
    List<Map<String, Object>> allColumnsRows = new ArrayList<>();

    for (Keyspace keyspace : keyspaces) {

      Map<String, Object> keyspaceRow = new HashMap<>();
      keyspaceRow.put("keyspace_name", keyspace.name);
      keyspaceRow.put("durable_writes", true);
      keyspaceRow.put(
          "replication",
          ImmutableMap.of(
              "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));
      allKeyspacesRows.add(keyspaceRow);

      Query whenSelectKeyspace =
          new Query(SELECT_KEYSPACES + " WHERE keyspace_name = '" + keyspace.name + '\'');
      SuccessResult thenReturnKeyspace =
          new SuccessResult(Collections.singletonList(keyspaceRow), KEYSPACE_COLUMNS);
      RequestPrime primeKeyspace = new RequestPrime(whenSelectKeyspace, thenReturnKeyspace);
      simulacron.prime(new Prime(primeKeyspace));

      for (Table table : keyspace.tables) {

        Map<String, Object> tableRow = new HashMap<>();
        tableRow.put("keyspace_name", keyspace.name);
        tableRow.put("table_name", table.name);
        tableRow.put("bloom_filter_fp_chance", 0.01d);
        tableRow.put("caching", ImmutableMap.of("keys", "ALL", "rows_per_partition", "NONE"));
        tableRow.put("cdc", null);
        tableRow.put("comment", "");
        tableRow.put(
            "compaction",
            ImmutableMap.of(
                "class",
                "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                "max_threshold",
                "32",
                "min_threshold",
                "4"));
        tableRow.put(
            "compression",
            ImmutableMap.of(
                "chunk_length_in_kb",
                "64",
                "class",
                "org.apache.cassandra.io.compress.LZ4Compressor"));
        tableRow.put("crc_check_chance", 1d);
        tableRow.put("dclocal_read_repair_chance", 0.1d);
        tableRow.put("default_time_to_live", 0);
        // for some strange reason this column makes simulacron hang
        // tableRow.put("extensions", null);
        tableRow.put("flags", ImmutableSet.of("compound"));
        tableRow.put("gc_grace_seconds", 864000);
        tableRow.put("id", UUID.randomUUID());
        tableRow.put("max_index_interval", 2048);
        tableRow.put("memtable_flush_period_in_ms", 0);
        tableRow.put("min_index_interval", 128);
        tableRow.put("read_repair_chance", 0d);
        tableRow.put("speculative_retry", "99PERCENTILE");
        allTablesRows.add(tableRow);

        Query whenSelectTable =
            new Query(
                SELECT_TABLES
                    + " WHERE keyspace_name = '"
                    + keyspace.name
                    + "'  AND table_name = '"
                    + table.name
                    + '\'');
        SuccessResult thenReturnTable =
            new SuccessResult(Collections.singletonList(tableRow), TABLE_COLUMNS);
        RequestPrime primeTable = new RequestPrime(whenSelectTable, thenReturnTable);
        simulacron.prime(new Prime(primeTable));

        List<Map<String, Object>> tableColumnsRows = new ArrayList<>();

        int position = 0;
        for (Column column : table.partitionKey) {
          Map<String, Object> columnRow = new HashMap<>();
          columnRow.put("keyspace_name", keyspace.name);
          columnRow.put("table_name", table.name);
          columnRow.put("column_name", column.name);
          columnRow.put("clustering_order", "none");
          columnRow.put("column_name_bytes", column.name.getBytes(StandardCharsets.UTF_8));
          columnRow.put("kind", "partition_key");
          columnRow.put("position", position++);
          columnRow.put("type", column.type.toString());
          tableColumnsRows.add(columnRow);
        }

        position = 0;
        for (Column column : table.clusteringColumns) {
          Map<String, Object> columnRow = new HashMap<>();
          columnRow.put("keyspace_name", keyspace.name);
          columnRow.put("table_name", table.name);
          columnRow.put("column_name", column.name);
          columnRow.put("clustering_order", "asc");
          columnRow.put("column_name_bytes", column.name.getBytes(StandardCharsets.UTF_8));
          columnRow.put("kind", "clustering");
          columnRow.put("position", position++);
          columnRow.put("type", column.type.toString());
          tableColumnsRows.add(columnRow);
        }

        for (Column column : table.otherColumns) {
          Map<String, Object> columnRow = new HashMap<>();
          columnRow.put("keyspace_name", keyspace.name);
          columnRow.put("table_name", table.name);
          columnRow.put("column_name", column.name);
          columnRow.put("clustering_order", "none");
          columnRow.put("column_name_bytes", column.name.getBytes(StandardCharsets.UTF_8));
          columnRow.put("kind", "regular");
          columnRow.put("position", -1);
          columnRow.put("type", column.type.toString());
          tableColumnsRows.add(columnRow);
        }

        Query whenSelectTableColumns =
            new Query(
                SELECT_COLUMNS
                    + " WHERE keyspace_name = '"
                    + keyspace.name
                    + "'  AND table_name = '"
                    + table.name
                    + '\'');
        SuccessResult thenReturnTableColumns = new SuccessResult(tableColumnsRows, TABLE_COLUMNS);
        RequestPrime primeAllTableColumns =
            new RequestPrime(whenSelectTableColumns, thenReturnTableColumns);
        simulacron.prime(new Prime(primeAllTableColumns));

        allColumnsRows.addAll(tableColumnsRows);

        // INSERT INTO table
        Query whenInsertIntoTable =
            new Query(
                String.format(
                    "INSERT INTO %s.%s (%s) VALUES (%s)",
                    CqlIdentifier.fromInternal(keyspace.name).asCql(true),
                    CqlIdentifier.fromInternal(table.name).asCql(true),
                    table
                        .allColumns()
                        .stream()
                        .map(col -> CqlIdentifier.fromInternal(col.name).asCql(true))
                        .collect(Collectors.joining(",")),
                    table
                        .allColumns()
                        .stream()
                        .map(col -> ":" + CqlIdentifier.fromInternal(col.name).asCql(true))
                        .collect(Collectors.joining(","))),
                emptyList(),
                emptyMap(),
                table.allColumnTypes());
        simulacron.prime(
            new Prime(
                new RequestPrime(whenInsertIntoTable, new SuccessResult(emptyList(), emptyMap()))));

        // UPDATE table
        Query whenUpdateIntoTable =
            new Query(
                String.format(
                    "UPDATE %s.%s SET %s",
                    CqlIdentifier.fromInternal(keyspace.name).asCql(true),
                    CqlIdentifier.fromInternal(table.name).asCql(true),
                    table
                        .allColumns()
                        .stream()
                        .map(
                            col ->
                                CqlIdentifier.fromInternal(col.name).asCql(true)
                                    + "=:"
                                    + CqlIdentifier.fromInternal(col.name).asCql(true))
                        .collect(Collectors.joining(","))),
                emptyList(),
                emptyMap(),
                table.allColumnTypes());
        simulacron.prime(
            new Prime(
                new RequestPrime(whenUpdateIntoTable, new SuccessResult(emptyList(), emptyMap()))));

        // SELECT from table
        Query whenSelectFromTable =
            new Query(
                String.format(
                    "SELECT %s FROM %s.%s",
                    table
                        .allColumns()
                        .stream()
                        .map(col -> CqlIdentifier.fromInternal(col.name).asCql(true))
                        .collect(Collectors.joining(",")),
                    CqlIdentifier.fromInternal(keyspace.name).asCql(true),
                    CqlIdentifier.fromInternal(table.name).asCql(true)));
        simulacron.prime(
            new Prime(
                new RequestPrime(
                    whenSelectFromTable, new SuccessResult(table.rows, table.allColumnTypes()))));
      }
    }

    Query whenSelectAllKeyspaces = new Query(SELECT_KEYSPACES);
    SuccessResult thenReturnAllKeyspaces = new SuccessResult(allKeyspacesRows, KEYSPACE_COLUMNS);
    RequestPrime primeAllKeyspaces =
        new RequestPrime(whenSelectAllKeyspaces, thenReturnAllKeyspaces);
    simulacron.prime(new Prime(primeAllKeyspaces));

    Query whenSelectAllTables = new Query(SELECT_TABLES);
    SuccessResult thenReturnAllTables = new SuccessResult(allTablesRows, TABLE_COLUMNS);
    RequestPrime primeAllTables = new RequestPrime(whenSelectAllTables, thenReturnAllTables);
    simulacron.prime(new Prime(primeAllTables));

    Query whenSelectAllColumns = new Query(SELECT_COLUMNS);
    SuccessResult thenReturnAllColumns = new SuccessResult(allColumnsRows, COLUMN_COLUMNS);
    RequestPrime primeAllColumns = new RequestPrime(whenSelectAllColumns, thenReturnAllColumns);
    simulacron.prime(new Prime(primeAllColumns));
  }
}
