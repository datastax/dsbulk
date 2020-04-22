/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.simulacron;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.ServerErrorResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.assertj.core.util.Sets;

public class SimulacronUtils {

  private static final String SELECT_KEYSPACES = "SELECT * FROM system_schema.keyspaces";
  private static final String SELECT_TABLES = "SELECT * FROM system_schema.tables";
  private static final String SELECT_COLUMNS = "SELECT * FROM system_schema.columns";

  private static final String SELECT_SYSTEM_LOCAL = "SELECT * FROM system.local";
  private static final String SELECT_SYSTEM_PEERS = "SELECT * FROM system.peers";
  private static final String SELECT_SYSTEM_PEERS_V2 = "SELECT * FROM system.peers_v2";

  private static final String SELECT_SYSTEM_LOCAL_SCHEMA_VERSION =
      "SELECT schema_version FROM system.local WHERE key='local'";
  private static final String SELECT_SYSTEM_PEERS_SCHEMA_VERSION =
      "SELECT host_id, schema_version FROM system.peers";

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
          .put("extensions", "map<varchar, blob>")
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

  private static final ImmutableMap<String, String> SYSTEM_LOCAL_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("key", "varchar")
          .put("bootstrapped", "varchar")
          .put("broadcast_address", "inet")
          .put("cluster_name", "varchar")
          .put("cql_version", "varchar")
          .put("data_center", "varchar")
          .put("dse_version", "varchar")
          .put("gossip_generation", "int")
          .put("graph", "boolean")
          .put("host_id", "uuid")
          .put("jmx_port", "int")
          .put("listen_address", "inet")
          .put("native_protocol_version", "varchar")
          .put("native_transport_address", "inet")
          .put("native_transport_port", "int")
          .put("native_transport_port_ssl", "int")
          .put("partitioner", "varchar")
          .put("rack", "varchar")
          .put("release_version", "varchar")
          .put("rpc_address", "inet")
          .put("schema_version", "uuid")
          .put("server_id", "varchar")
          .put("storage_port", "int")
          .put("storage_port_ssl", "int")
          .put("tokens", "set<varchar>")
          .put("truncated_at", "map<uuid, blob>")
          .put("workload", "varchar")
          // Simulacron does not handle frozen collections
          .put("workloads", /*"frozen<set<varchar>>"*/ "set<varchar>")
          .build();

  private static final ImmutableMap<String, Object> SYSTEM_LOCAL_ROW =
      ImmutableMap.<String, Object>builder()
          .put("key", "local")
          .put("bootstrapped", "COMPLETED")
          .put("cql_version", "3.4.5")
          .put("data_center", "dc1")
          .put("dse_version", "5.0.0")
          .put("gossip_generation", 1532880775)
          .put("graph", false)
          .put("host_id", UUID.randomUUID())
          .put("jmx_port", "7100")
          .put("native_protocol_version", DefaultProtocolVersion.V4)
          .put("native_transport_port", 9042)
          .put("native_transport_port_ssl", 9042)
          .put("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
          .put("rack", "rack1")
          .put("release_version", "4.0.0.2284")
          .put("schema_version", UUID.randomUUID())
          .put("server_id", "8C-85-90-1A-3E-7A")
          .put("storage_port", 7000)
          .put("storage_port_ssl", 7001)
          .put("tokens", Sets.newLinkedHashSet("-9223372036854775808"))
          .put("truncated_at", new HashMap<>())
          .put("workload", "Cassandra")
          .put("workloads", Sets.newLinkedHashSet("Cassandra"))
          .build();

  private static final ImmutableMap<String, String> SYSTEM_PEERS_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("peer", "inet")
          .put("data_center", "varchar")
          .put("host_id", "uuid")
          .put("preferred_ip", "inet")
          .put("rack", "varchar")
          .put("release_version", "varchar")
          .put("rpc_address", "inet")
          .put("schema_version", "uuid")
          .put("tokens", "set<varchar>")
          .build();

  private static final ImmutableMap<String, Object> SYSTEM_PEERS_ROW =
      ImmutableMap.<String, Object>builder()
          .put("data_center", "dc1")
          .put("host_id", UUID.randomUUID())
          .put("preferred_ip", InetSocketAddress.createUnresolved("1.2.3.4", 9042))
          .put("rack", "rack1")
          .put("release_version", "4.0.0.2284")
          .put("schema_version", UUID.randomUUID())
          .put("tokens", Sets.newLinkedHashSet("-9223372036854775808"))
          .build();

  private static final Collector<CharSequence, ?, String> COMMA = Collectors.joining(", ");

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
      return allColumns().stream()
          .map(col -> new SimpleEntry<>(col.name, col.getTypeAsString()))
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

    String getTypeAsString() {
      return type == TEXT ? "varchar" : type.toString().toLowerCase();
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
        tableRow.put("extensions", null);
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
          columnRow.put("type", column.getTypeAsString());
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
          columnRow.put("type", column.getTypeAsString());
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
          columnRow.put("type", column.getTypeAsString());
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
                    asCql(keyspace.name),
                    asCql(table.name),
                    table.allColumns().stream().map(col -> asCql(col.name)).collect(COMMA),
                    table.allColumns().stream().map(col -> ":" + asCql(col.name)).collect(COMMA)),
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
                    asCql(keyspace.name),
                    asCql(table.name),
                    table.allColumns().stream()
                        .map(col -> asCql(col.name) + "=:" + asCql(col.name))
                        .collect(COMMA)),
                emptyList(),
                emptyMap(),
                table.allColumnTypes());
        simulacron.prime(
            new Prime(
                new RequestPrime(whenUpdateIntoTable, new SuccessResult(emptyList(), emptyMap()))));

        // SELECT cols from table
        Query whenSelectFromTable =
            new Query(
                String.format(
                    "SELECT %s FROM %s.%s",
                    table.allColumns().stream().map(col -> asCql(col.name)).collect(COMMA),
                    asCql(keyspace.name),
                    asCql(table.name)));
        simulacron.prime(
            new Prime(
                new RequestPrime(
                    whenSelectFromTable, new SuccessResult(table.rows, table.allColumnTypes()))));

        // SELECT from table WHERE token...
        Query whenSelectFromTableWhere =
            new Query(
                String.format(
                    "SELECT %s FROM %s.%s WHERE token(%s) > ? AND token(%s) <= ?",
                    table.allColumns().stream().map(col -> asCql(col.name)).collect(COMMA),
                    asCql(keyspace.name),
                    asCql(table.name),
                    table.partitionKey.stream().map(col -> asCql(col.name)).collect(COMMA),
                    table.partitionKey.stream().map(col -> asCql(col.name)).collect(COMMA)));

        simulacron.prime(
            new Prime(
                new RequestPrime(
                    whenSelectFromTableWhere,
                    new SuccessResult(table.rows, table.allColumnTypes()))));
        whenSelectFromTableWhere =
            new Query(
                String.format(
                    "SELECT %s FROM %s.%s WHERE token(%s) > :start AND token(%s) <= :end",
                    table.allColumns().stream().map(col -> asCql(col.name)).collect(COMMA),
                    asCql(keyspace.name),
                    asCql(table.name),
                    table.partitionKey.stream().map(col -> asCql(col.name)).collect(COMMA),
                    table.partitionKey.stream().map(col -> asCql(col.name)).collect(COMMA)));
        simulacron.prime(
            new Prime(
                new RequestPrime(
                    whenSelectFromTableWhere,
                    new SuccessResult(table.rows, table.allColumnTypes()))));
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

  public static void primeSystemLocal(BoundCluster simulacron, Map<String, Object> overrides) {
    {
      Query whenSelectSystemLocal = new Query(SELECT_SYSTEM_LOCAL);
      List<Map<String, Object>> systemLocalResultSet = new ArrayList<>();
      Map<String, Object> row = new HashMap<>(SYSTEM_LOCAL_ROW);
      row.putAll(overrides);
      // The following columns cannot be overridden and must have values that match the simulacron
      // cluster
      InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
      row.put("cluster_name", simulacron.getName());
      row.put("broadcast_address", node.getAddress());
      row.put("listen_address", node.getAddress());
      row.put("native_transport_address", node.getAddress());
      row.put("rpc_address", node.getAddress());
      row.put("native_transport_port", node.getPort());
      systemLocalResultSet.add(row);
      SuccessResult thenReturnLocalRow =
          new SuccessResult(systemLocalResultSet, SYSTEM_LOCAL_COLUMNS);
      RequestPrime primeSystemLocal = new RequestPrime(whenSelectSystemLocal, thenReturnLocalRow);
      simulacron.prime(new Prime(primeSystemLocal));
    }
    {
      Query whenSelectSystemLocal = new Query(SELECT_SYSTEM_LOCAL_SCHEMA_VERSION);
      List<Map<String, Object>> systemLocalResultSet = new ArrayList<>();
      Map<String, Object> row = new HashMap<>(SYSTEM_LOCAL_ROW);
      systemLocalResultSet.add(row);
      SuccessResult thenReturnLocalRow =
          new SuccessResult(systemLocalResultSet, SYSTEM_LOCAL_COLUMNS);
      RequestPrime primeSystemLocal = new RequestPrime(whenSelectSystemLocal, thenReturnLocalRow);
      simulacron.prime(new Prime(primeSystemLocal));
    }
  }

  public static void primeSystemPeers(BoundCluster simulacron) {
    {
      Query whenSelectSystemPeers = new Query(SELECT_SYSTEM_PEERS);
      List<Map<String, Object>> systemPeersResultSet = new ArrayList<>();
      boolean local = true;
      for (BoundNode node : simulacron.getNodes()) {
        if (local) {
          local = false;
        } else {
          Map<String, Object> row = new HashMap<>(SYSTEM_PEERS_ROW);
          // The following columns cannot be overridden and must have values that match the
          // simulacron cluster
          InetSocketAddress addr = node.inetSocketAddress();
          row.put("peer", addr.getAddress());
          row.put("preferred_ip", addr.getAddress());
          row.put("rpc_address", addr.getAddress());
          systemPeersResultSet.add(row);
        }
      }
      SuccessResult thenReturnLocalRow =
          new SuccessResult(systemPeersResultSet, SYSTEM_PEERS_COLUMNS);
      RequestPrime primeSystemLocal = new RequestPrime(whenSelectSystemPeers, thenReturnLocalRow);
      simulacron.prime(new Prime(primeSystemLocal));
    }
    {
      Query whenSelectSystemPeers = new Query(SELECT_SYSTEM_PEERS_SCHEMA_VERSION);
      List<Map<String, Object>> systemPeersResultSet = new ArrayList<>();
      boolean local = true;
      for (BoundNode node : simulacron.getNodes()) {
        if (local) {
          local = false;
        } else {
          Map<String, Object> row = new HashMap<>(SYSTEM_PEERS_ROW);
          // The following columns cannot be overridden and must have values that match the
          // simulacron cluster
          InetSocketAddress addr = node.inetSocketAddress();
          row.put("peer", addr.getAddress());
          row.put("preferred_ip", addr.getAddress());
          row.put("rpc_address", addr.getAddress());
          systemPeersResultSet.add(row);
        }
      }
      SuccessResult thenReturnLocalRow =
          new SuccessResult(systemPeersResultSet, SYSTEM_PEERS_COLUMNS);
      RequestPrime primeSystemLocal = new RequestPrime(whenSelectSystemPeers, thenReturnLocalRow);
      simulacron.prime(new Prime(primeSystemLocal));
    }
  }

  public static void primeSystemPeersV2(BoundCluster simulacron) {
    Query whenSelectSystemPeersV2 = new Query(SELECT_SYSTEM_PEERS_V2);
    ErrorResult thenThrowServerError =
        new ServerErrorResult("Unknown keyspace/cf pair (system.peers_v2)");
    RequestPrime primeSystemPeersV2 =
        new RequestPrime(whenSelectSystemPeersV2, thenThrowServerError);
    simulacron.prime(new Prime(primeSystemPeersV2));
  }

  private static String asCql(String name) {
    return CqlIdentifier.fromInternal(name).asCql(true);
  }
}
