/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DriverCoreHooks.resultSetVariables;
import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.dsbulk.engine.WorkflowType.COUNT;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.checkGraphCompatibility;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.EdgeMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.VertexMetadata;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.partitioner.TokenRangeReadStatementGenerator;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.CQLFragment;
import com.datastax.dsbulk.engine.internal.schema.CQLIdentifier;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.FunctionCall;
import com.datastax.dsbulk.engine.internal.schema.IndexedMappingField;
import com.datastax.dsbulk.engine.internal.schema.MappedMappingField;
import com.datastax.dsbulk.engine.internal.schema.MappingField;
import com.datastax.dsbulk.engine.internal.schema.MappingInspector;
import com.datastax.dsbulk.engine.internal.schema.QueryInspector;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSettings.class);

  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String KEYSPACE = "keyspace";
  private static final String GRAPH = "graph";
  private static final String TABLE = "table";
  private static final String VERTEX = "vertex";
  private static final String EDGE = "edge";
  private static final String FROM = "from";
  private static final String TO = "to";
  private static final String MAPPING = "mapping";
  private static final String ALLOW_EXTRA_FIELDS = "allowExtraFields";
  private static final String ALLOW_MISSING_FIELDS = "allowMissingFields";
  private static final String QUERY = "query";
  private static final String QUERY_TTL = "queryTtl";
  private static final String QUERY_TIMESTAMP = "queryTimestamp";
  private static final String NATIVE = "Native";

  private final LoaderConfig config;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private MappingInspector mapping;
  private int ttlSeconds;
  private long timestampMicros;
  private TableMetadata table;
  private KeyspaceMetadata keyspace;
  private String keyspaceName;
  private String tableName;
  private String query;
  private QueryInspector queryInspector;
  private PreparedStatement preparedStatement;
  private ImmutableSet<CQLFragment> writeTimeVariables;
  private boolean preferIndexedMapping;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init(WorkflowType workflowType, Cluster cluster, boolean expectIndexedMapping) {
    try {

      preferIndexedMapping = expectIndexedMapping;

      // Sanity Checks

      if (config.hasPath(KEYSPACE) && config.hasPath(GRAPH)) {
        throw new BulkConfigurationException(
            "Settings schema.keyspace and schema.graph are mutually exclusive");
      }
      if (config.hasPath(TABLE) && config.hasPath(VERTEX)) {
        throw new BulkConfigurationException(
            "Settings schema.table and schema.vertex are mutually exclusive");
      }
      if (config.hasPath(TABLE) && config.hasPath(EDGE)) {
        throw new BulkConfigurationException(
            "Settings schema.table and schema.edge are mutually exclusive");
      }
      if (config.hasPath(VERTEX) && config.hasPath(EDGE)) {
        throw new BulkConfigurationException(
            "Settings schema.vertex and schema.edge are mutually exclusive");
      }
      if (config.hasPath(EDGE)) {
        if (!config.hasPath(FROM)) {
          throw new BulkConfigurationException(
              "Setting schema.from is required when schema.edge is specified");
        }
        if (!config.hasPath(TO)) {
          throw new BulkConfigurationException(
              "Setting schema.to is required when schema.edge is specified");
        }
      }
      if (config.hasPath(QUERY)
          && (config.hasPath(TABLE) || config.hasPath(VERTEX) || config.hasPath(EDGE))) {
        throw new BulkConfigurationException(
            "Setting schema.query must not be defined if schema.table, schema.vertex or schema.edge are defined");
      }
      if ((!config.hasPath(KEYSPACE) && !config.hasPath(GRAPH))
          && (config.hasPath(TABLE) || config.hasPath(VERTEX) || config.hasPath(EDGE))) {
        throw new BulkConfigurationException(
            "Settings schema.keyspace or schema.graph must be defined if schema.table, schema.vertex or schema.edge are defined");
      }

      // Keyspace

      if (config.hasPath(KEYSPACE)) {
        keyspace = locateKeyspace(cluster.getMetadata(), config.getString(KEYSPACE));
      } else if (config.hasPath(GRAPH)) {
        keyspace = locateKeyspace(cluster.getMetadata(), config.getString(GRAPH));
      }

      // Table

      if (keyspace != null) {
        if (config.hasPath(TABLE)) {
          table = locateTable(keyspace, config.getString(TABLE));
        } else if (config.hasPath(VERTEX)) {
          table = locateVertexTable(keyspace, config.getString(VERTEX));
        } else if (config.hasPath(EDGE)) {
          table =
              locateEdgeTable(
                  keyspace, config.getString(EDGE), config.getString(FROM), config.getString(TO));
        }
      }

      // Timestamp and TTL

      ttlSeconds = config.getInt(QUERY_TTL);
      if (config.hasPath(QUERY_TIMESTAMP)) {
        String timestampStr = config.getString(QUERY_TIMESTAMP);
        try {
          Instant instant = ZonedDateTime.parse(timestampStr).toInstant();
          this.timestampMicros = instantToNumber(instant, MICROSECONDS, EPOCH);
        } catch (Exception e) {
          throw new BulkConfigurationException(
              String.format(
                  "Expecting schema.queryTimestamp to be in ISO_ZONED_DATE_TIME format but got '%s'",
                  timestampStr));
        }
      } else {
        this.timestampMicros = -1L;
      }

      // Custom Query

      if (config.hasPath(QUERY)) {

        query = config.getString(QUERY);
        queryInspector = new QueryInspector(query);

        if (queryInspector.getKeyspaceName().isPresent()) {
          if (keyspace != null) {
            throw new BulkConfigurationException(
                "Setting schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
          }
          CQLIdentifier keyspaceName = queryInspector.getKeyspaceName().get();
          keyspace = cluster.getMetadata().getKeyspace(keyspaceName.asCql());
          if (keyspace == null) {
            throw new BulkConfigurationException(
                String.format(
                    "Value for schema.query references a non-existent keyspace: %s",
                    keyspaceName.asCql()));
          }
        } else if (keyspace == null) {
          throw new BulkConfigurationException(
              "Setting schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
        }

        CQLIdentifier tableName = queryInspector.getTableName();
        table = keyspace.getTable(tableName.asCql());
        if (table == null) {
          throw new BulkConfigurationException(
              String.format(
                  "Value for schema.query references a non-existent table: %s", tableName.asCql()));
        }

        // If a query is provided, ttl and timestamp must not be.
        if (timestampMicros != -1 || ttlSeconds != -1) {
          throw new BulkConfigurationException(
              "Setting schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
        }

        // If a query is provided, check now if it contains a USING TIMESTAMP variable,
        // or selectors containing a writetime() function call, and get their names.
        writeTimeVariables = queryInspector.getWriteTimeVariables();

      } else {

        writeTimeVariables = ImmutableSet.of();

        if (keyspace == null || table == null) {

          // Either the keyspace and table must be present, or the query must be present.
          throw new BulkConfigurationException(
              "When schema.query is not defined, "
                  + "then either schema.keyspace or schema.graph must be defined, "
                  + "and either schema.table, schema.vertex or schema.edge must be defined");
        }
      }

      assert keyspace != null;
      assert table != null;

      keyspaceName = quoteIfNecessary(keyspace.getName());
      tableName = quoteIfNecessary(table.getName());

      // Mapping

      if (config.hasPath(MAPPING)) {

        if (workflowType == COUNT) {
          throw new BulkConfigurationException(
              "Setting schema.mapping must not be defined when counting rows in a table");
        }

        mapping = new MappingInspector(config.getString(MAPPING), preferIndexedMapping);

        if (preferIndexedMapping && !mapping.isIndexed()) {
          throw new BulkConfigurationException(
              "Schema mapping contains named fields, but connector only supports indexed fields, "
                  + "please enable support for named fields in the connector, or alternatively, "
                  + "provide an indexed mapping of the form: '0=col1,1=col2,...'");
        }

        // Error out if the explicit variables map timestamp or ttl and
        // there is an explicit query.
        if (query != null) {
          if (mapping.getExplicitVariables().containsValue(INTERNAL_TIMESTAMP_VARNAME)) {
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when mapping a field to query-timestamp");
          }
          if (mapping.getExplicitVariables().containsValue(INTERNAL_TTL_VARNAME)) {
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when mapping a field to query-ttl");
          }
          if (mapping
              .getExplicitVariables()
              .keySet()
              .stream()
              .anyMatch(FunctionCall.class::isInstance)) {
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when mapping a function to a column");
          }
          if (mapping
              .getExplicitVariables()
              .values()
              .stream()
              .anyMatch(FunctionCall.class::isInstance)) {
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when mapping a function to a column");
          }
        }

        // merge the write time variable name if it was present in the mapping with
        // the ones found in the query.
        writeTimeVariables =
            ImmutableSet.<CQLFragment>builder()
                .addAll(writeTimeVariables)
                .addAll(mapping.getWriteTimeVariables())
                .build();
      }

      // Misc

      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      allowExtraFields = config.getBoolean(ALLOW_EXTRA_FIELDS);
      allowMissingFields = config.getBoolean(ALLOW_MISSING_FIELDS);

      // Final checks related to graph operations

      if (hasGraphOptions(config)) {

        checkGraphCompatibility(cluster);

        if (!isGraph(keyspace)) {
          throw new IllegalStateException(
              "Graph operations requested but provided keyspace is not a graph: " + keyspaceName);
        }
        if (!isSupportedGraph(keyspace)) {
          throw new IllegalStateException(
              String.format(
                  "Graph operations requested but provided graph %s was created with an unsupported graph engine: %s",
                  keyspaceName, keyspace.getGraphEngine()));
        }

      } else if (isGraph(keyspace)) {

        if (isSupportedGraph(keyspace)) {
          if (config.hasPath(KEYSPACE) || config.hasPath(TABLE)) {
            LOGGER.warn(
                "Provided keyspace is a graph; "
                    + "instead of schema.keyspace and schema.table, please use graph-specific options "
                    + "such as schema.graph, schema.vertex, schema.edge, schema.from and schema.to.");
          }
        } else {
          if (workflowType == LOAD) {
            LOGGER.warn(
                "Provided keyspace is a graph created with a legacy graph engine: "
                    + keyspace.getGraphEngine()
                    + "; attempting to load data into such a keyspace is not supported and "
                    + "may put the graph in an inconsistent state.");
          }
        }
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "schema");
    }
  }

  public RecordMapper createRecordMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(
            session, codecRegistry, LOAD, EnumSet.noneOf(StatisticsMode.class));
    return new DefaultRecordMapper(
        preparedStatement,
        primaryKeyVariables(),
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields);
  }

  public ReadResultMapper createReadResultMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    // wo don't check that mapping records are supported when unloading, the only thing that matters
    // is the order in which fields appear in the record.
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(
            session, codecRegistry, UNLOAD, EnumSet.noneOf(StatisticsMode.class));
    return new DefaultReadResultMapper(mapping, recordMetadata);
  }

  public ReadResultCounter createReadResultCounter(
      Session session,
      ExtendedCodecRegistry codecRegistry,
      EnumSet<StatsSettings.StatisticsMode> modes,
      int numPartitions) {
    prepareStatementAndCreateMapping(session, null, COUNT, modes);
    Cluster cluster = session.getCluster();
    ProtocolVersion protocolVersion =
        cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    Metadata metadata = cluster.getMetadata();
    if (modes.contains(partitions) && table.getClusteringColumns().isEmpty()) {
      throw new BulkConfigurationException(
          String.format(
              "Cannot count partitions for table %s: it has no clustering column.", tableName));
    }
    return new DefaultReadResultCounter(
        keyspaceName, metadata, modes, numPartitions, protocolVersion, codecRegistry);
  }

  public List<? extends Statement> createReadStatements(Cluster cluster, int splitCount) {
    ColumnDefinitions variables = preparedStatement.getVariables();
    if (variables.size() == 0) {
      return Collections.singletonList(preparedStatement.bind());
    }
    List<String> unrecognized =
        StreamSupport.stream(variables.spliterator(), false)
            .map(ColumnDefinitions.Definition::getName)
            .filter(name -> !name.equals("start") && !name.equals("end"))
            .map(Metadata::quoteIfNecessary)
            .collect(Collectors.toList());
    if (!unrecognized.isEmpty()) {
      throw new BulkConfigurationException(
          String.format(
              "The provided statement (schema.query) contains unrecognized bound variables: %s; "
                  + "only 'start' and 'end' can be used to define a token range",
              String.join(", ", unrecognized)));
    }
    Metadata metadata = cluster.getMetadata();
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    return generator.generate(
        splitCount,
        range -> {
          LOGGER.debug("Generating bound statement for token range: {}", range);
          return preparedStatement
              .bind()
              .setToken("start", metadata.newToken(range.start().toString()))
              .setToken("end", metadata.newToken(range.end().toString()));
        });
  }

  @NotNull
  public RowType getRowType() {
    if (table.getVertexMetadata() != null) {
      return RowType.VERTEX;
    } else if (table.getEdgeMetadata() != null) {
      return RowType.EDGE;
    } else {
      return RowType.REGULAR;
    }
  }

  @NotNull
  private DefaultMapping prepareStatementAndCreateMapping(
      Session session,
      ExtendedCodecRegistry codecRegistry,
      WorkflowType workflowType,
      EnumSet<StatsSettings.StatisticsMode> modes) {
    BiMap<MappingField, CQLFragment> fieldsToVariables = null;
    if (!config.hasPath(QUERY)) {
      // in the absence of user-provided queries, create the mapping *before* query generation and
      // preparation
      fieldsToVariables =
          createFieldsToVariablesMap(
              table
                  .getColumns()
                  .stream()
                  .map(ColumnMetadata::getName)
                  .map(CQLIdentifier::fromInternal)
                  .collect(Collectors.toList()));
      // query generation
      if (workflowType == LOAD) {
        if (isCounterTable()) {
          query = inferUpdateCounterQuery(fieldsToVariables);
        } else {
          query = inferInsertQuery(fieldsToVariables);
        }
      } else if (workflowType == UNLOAD) {
        query = inferReadQuery(fieldsToVariables);
      } else if (workflowType == COUNT) {
        query = inferCountQuery(modes);
      }
      LOGGER.debug("Inferred query: {}", query);
      queryInspector = new QueryInspector(query);
      // validate generated query
      if (workflowType == LOAD) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
      fieldsToVariables = processMappingFunctions(fieldsToVariables, workflowType);
    }
    assert query != null;
    assert queryInspector != null;
    if (!queryInspector.getKeyspaceName().isPresent()) {
      session.execute("USE " + keyspaceName);
    }
    // Transform user-provided queries before preparation
    if (config.hasPath(QUERY)) {
      if ((workflowType == UNLOAD || workflowType == COUNT) && !queryInspector.hasWhereClause()) {
        int whereClauseIndex = queryInspector.getFromClauseEndIndex() + 1;
        StringBuilder sb = new StringBuilder(query.substring(0, whereClauseIndex));
        appendTokenRangeRestriction(sb);
        query = sb.append(query.substring(whereClauseIndex)).toString();
      }
      if (workflowType == COUNT) {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (modes.contains(partitions)) {
          appendPartitionKey(sb);
        } else if (modes.contains(ranges) || modes.contains(hosts)) {
          appendTokenFunction(sb);
        } else {
          sb.append(getGlobalCountSelector());
        }
        query =
            sb.append(' ')
                .append(query.substring(queryInspector.getFromClauseStartIndex()))
                .toString();
      }
    }
    preparedStatement = session.prepare(query);
    if (config.hasPath(QUERY)) {
      // in the presence of user-provided queries, create the mapping *after* query preparation
      ColumnDefinitions variables = getVariables(workflowType);
      fieldsToVariables =
          createFieldsToVariablesMap(
              StreamSupport.stream(variables.spliterator(), false)
                  .map(ColumnDefinitions.Definition::getName)
                  .map(CQLIdentifier::fromInternal)
                  .collect(Collectors.toList()));
      // validate user-provided query
      if (workflowType == LOAD) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
    }
    assert fieldsToVariables != null;
    return new DefaultMapping(
        ImmutableBiMap.copyOf(fieldsToVariables), codecRegistry, writeTimeVariables);
  }

  private boolean isCounterTable() {
    return table
        .getColumns()
        .stream()
        .anyMatch(c -> c.getType().getName() == DataType.Name.COUNTER);
  }

  private ColumnDefinitions getVariables(WorkflowType workflowType) {
    switch (workflowType) {
      case LOAD:
        return preparedStatement.getVariables();
      case UNLOAD:
      case COUNT:
        return resultSetVariables(preparedStatement);
      default:
        throw new AssertionError();
    }
  }

  private BiMap<MappingField, CQLFragment> createFieldsToVariablesMap(List<CQLFragment> columns)
      throws BulkConfigurationException {
    BiMap<MappingField, CQLFragment> fieldsToVariables;
    // create indexed mappings only for unload, and only if the connector really requires it, to
    // match the order in which the query declares variables.
    if (mapping == null) {
      fieldsToVariables = inferFieldsToVariablesMap(columns);
    } else {
      if (mapping.isInferring()) {
        fieldsToVariables = inferFieldsToVariablesMap(columns);
      } else {
        fieldsToVariables = HashBiMap.create();
      }

      ImmutableBiMap<MappingField, CQLFragment> explicitVariables = mapping.getExplicitVariables();
      if (!explicitVariables.isEmpty()) {
        ImmutableBiMap.Builder<MappingField, CQLFragment> builder = ImmutableBiMap.builder();
        for (Map.Entry<MappingField, CQLFragment> entry : explicitVariables.entrySet()) {
          builder.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entrySet()) {
          if (!explicitVariables.containsKey(entry.getKey())
              && !explicitVariables.containsValue(entry.getValue())) {
            builder.put(entry.getKey(), entry.getValue());
          }
        }
        fieldsToVariables = builder.build();
      }
    }

    Preconditions.checkState(
        !fieldsToVariables.isEmpty(),
        "Mapping was absent and could not be inferred, please provide an explicit mapping");

    validateAllFieldsPresent(fieldsToVariables, columns);

    return fieldsToVariables;
  }

  private KeyspaceMetadata locateKeyspace(Metadata metadata, String keyspaceName) {
    KeyspaceMetadata keyspace = metadata.getKeyspace(quoteIfNecessary(keyspaceName));
    if (keyspace == null) {
      Optional<KeyspaceMetadata> match =
          metadata
              .getKeyspaces()
              .stream()
              .filter(k -> k.getName().equalsIgnoreCase(keyspaceName))
              .findFirst();
      if (match.isPresent()) {
        String similarName = quoteIfNecessary(match.get().getName());
        throw new BulkConfigurationException(
            String.format(
                "Keyspace %s does not exist, however a keyspace %s was found. Did you mean to use -k %s?",
                quoteIfNecessary(keyspaceName), similarName, similarName));
      } else {
        throw new BulkConfigurationException(
            String.format("Keyspace %s does not exist", quoteIfNecessary(keyspaceName)));
      }
    }
    return keyspace;
  }

  @NotNull
  private TableMetadata locateTable(KeyspaceMetadata keyspace, String tableName) {
    TableMetadata table = keyspace.getTable(quoteIfNecessary(tableName));
    if (table == null) {
      Optional<TableMetadata> match =
          keyspace
              .getTables()
              .stream()
              .filter(t -> t.getName().equalsIgnoreCase(tableName))
              .findFirst();
      if (match.isPresent()) {
        String similarName = quoteIfNecessary(match.get().getName());
        throw new BulkConfigurationException(
            String.format(
                "Table %s does not exist, however a table %s was found. Did you mean to use -t %s?",
                quoteIfNecessary(tableName), similarName, similarName));
      } else {
        throw new BulkConfigurationException(
            String.format("Table %s does not exist", quoteIfNecessary(tableName)));
      }
    }
    return table;
  }

  @NotNull
  private TableMetadata locateVertexTable(KeyspaceMetadata keyspace, String vertexLabel) {
    Optional<TableMetadata> vertex =
        allVertexTables(keyspace)
            .filter(table -> table.getVertexMetadata().getLabelName().equals(vertexLabel))
            .findFirst();
    if (!vertex.isPresent()) {
      Optional<VertexMetadata> match =
          allVertexTables(keyspace)
              .filter(
                  table -> table.getVertexMetadata().getLabelName().equalsIgnoreCase(vertexLabel))
              .map(TableMetadata::getVertexMetadata)
              .findFirst();
      if (match.isPresent()) {
        String similarName = quoteIfNecessary(match.get().getLabelName());
        throw new BulkConfigurationException(
            String.format(
                "Vertex label %s does not exist, however a vertex label %s was found. Did you mean to use -v %s?",
                quoteIfNecessary(vertexLabel), similarName, similarName));
      } else {
        throw new BulkConfigurationException(
            String.format("Vertex label %s does not exist", quoteIfNecessary(vertexLabel)));
      }
    }
    return vertex.get();
  }

  @NotNull
  private TableMetadata locateEdgeTable(
      KeyspaceMetadata keyspace, String edgeLabel, String fromVertex, String toVertex) {
    Optional<TableMetadata> edge =
        allEdgeTables(keyspace)
            .filter(table -> table.getEdgeMetadata().getLabelName().equals(edgeLabel))
            .filter(table -> table.getEdgeMetadata().getFromLabel().equals(fromVertex))
            .filter(table -> table.getEdgeMetadata().getToLabel().equals(toVertex))
            .findFirst();
    if (!edge.isPresent()) {
      Optional<EdgeMetadata> match =
          allEdgeTables(keyspace)
              .map(TableMetadata::getEdgeMetadata)
              .filter(e -> e.getLabelName().equalsIgnoreCase(edgeLabel))
              .filter(e -> e.getFromLabel().equalsIgnoreCase(fromVertex))
              .filter(e -> e.getToLabel().equalsIgnoreCase(toVertex))
              .findFirst();
      if (match.isPresent()) {
        EdgeMetadata edgeMetadata = match.get();
        String similarLabel = quoteIfNecessary(edgeMetadata.getLabelName());
        String similarFrom = quoteIfNecessary(edgeMetadata.getFromLabel());
        String similarTo = quoteIfNecessary(edgeMetadata.getToLabel());
        throw new BulkConfigurationException(
            String.format(
                "Edge label %s from %s to %s does not exist, "
                    + "however an edge label %s from %s to %s was found. "
                    + "Did you mean to use -e %s -from %s -to %s?",
                quoteIfNecessary(edgeLabel),
                quoteIfNecessary(fromVertex),
                quoteIfNecessary(toVertex),
                similarLabel,
                similarFrom,
                similarTo,
                similarLabel,
                similarFrom,
                similarTo));
      } else {
        throw new BulkConfigurationException(
            String.format(
                "Edge label %s from %s to %s does not exist",
                quoteIfNecessary(edgeLabel),
                quoteIfNecessary(fromVertex),
                quoteIfNecessary(toVertex)));
      }
    }
    return edge.get();
  }

  private void validateAllFieldsPresent(
      BiMap<MappingField, CQLFragment> fieldsToVariables, List<CQLFragment> columns) {
    fieldsToVariables.forEach(
        (key, value) -> {
          if (value instanceof CQLIdentifier
              && !isPseudoColumn(value)
              && !columns.contains(value)) {
            if (!config.hasPath(QUERY)) {
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping entry '%s' doesn't match any column found in table %s",
                      value.asInternal(), tableName));
            } else {
              assert query != null;
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping entry '%s' doesn't match any bound variable found in query: '%s'",
                      value.asInternal(), query));
            }
          }
        });
  }

  private void validatePrimaryKeyPresent(BiMap<MappingField, CQLFragment> fieldsToVariables) {
    List<ColumnMetadata> partitionKey = table.getPrimaryKey();
    Set<CQLFragment> mappingVariables = fieldsToVariables.values();
    Map<CQLIdentifier, CQLFragment> queryVariables = queryInspector.getAssignments();
    for (ColumnMetadata pk : partitionKey) {
      CQLIdentifier pkVariable = CQLIdentifier.fromInternal(pk.getName());
      CQLFragment queryVariable = queryVariables.get(pkVariable);
      // the provided query did not contain such column
      if (queryVariable == null) {
        throw new BulkConfigurationException(
            "Missing required primary key column "
                + pkVariable.asCql()
                + " from schema.mapping or schema.query");
      }
      // do not check if the mapping contains a PK
      // if the PK is mapped to a function in the query (DAT-326)
      if (!(queryVariable instanceof FunctionCall)) {
        // the mapping did not contain such column
        if (!mappingVariables.contains(queryVariable)) {
          throw new BulkConfigurationException(
              "Missing required primary key column " + pkVariable.asCql() + " from schema.mapping");
        }
      }
    }
  }

  private ImmutableBiMap<MappingField, CQLFragment> inferFieldsToVariablesMap(
      List<CQLFragment> columns) {

    // use a builder to preserve iteration order
    ImmutableBiMap.Builder<MappingField, CQLFragment> fieldsToVariables =
        new ImmutableBiMap.Builder<>();

    int i = 0;
    for (CQLFragment colName : columns) {
      if (mapping == null || !mapping.getExcludedVariables().contains(colName)) {
        if (preferIndexedMapping) {
          fieldsToVariables.put(new IndexedMappingField(i), colName);
        } else {
          fieldsToVariables.put(new MappedMappingField(colName.asInternal()), colName);
        }
      }
      i++;
    }
    return fieldsToVariables.build();
  }

  private String inferInsertQuery(BiMap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    appendColumnNames(fieldsToVariables, sb, false);
    sb.append(") VALUES (");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    while (it.hasNext()) {
      CQLFragment col = it.next();
      if (isPseudoColumn(col)) {
        // This isn't a real column name.
        continue;
      }
      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      MappingField field = fieldsToVariables.inverse().get(col);
      if (field instanceof FunctionCall) {
        // append the function call as is
        sb.append(((FunctionCall) field).asCql());
      } else {
        sb.append(':');
        sb.append(col.asCql());
      }
    }
    sb.append(')');
    addTimestampAndTTL(fieldsToVariables, sb);
    return sb.toString();
  }

  private String inferUpdateCounterQuery(BiMap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(keyspaceName).append('.').append(tableName);
    // Note: TTL and timestamp are not allowed in counter queries;
    // a test is made inside the following method
    addTimestampAndTTL(fieldsToVariables, sb);
    sb.append(" SET ");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    List<CQLFragment> pks =
        table
            .getPrimaryKey()
            .stream()
            .map(ColumnMetadata::getName)
            .map(CQLIdentifier::fromInternal)
            .collect(Collectors.toList());
    while (it.hasNext()) {
      CQLFragment col = it.next();
      if (pks.contains(col)) {
        continue;
      }
      MappingField field = fieldsToVariables.inverse().get(col);
      if (field instanceof FunctionCall) {
        throw new BulkConfigurationException(
            "Function calls are not allowed when updating a counter table.");
      }
      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      sb.append(col.asCql()).append('=').append(col.asCql()).append("+:").append(col.asCql());
    }
    sb.append(" WHERE ");
    it = pks.iterator();
    isFirst = true;
    while (it.hasNext()) {
      CQLFragment col = it.next();
      if (!isFirst) {
        sb.append(" AND ");
      }
      isFirst = false;
      sb.append(col.asCql()).append("=:").append(col.asCql());
    }
    return sb.toString();
  }

  private void addTimestampAndTTL(
      BiMap<MappingField, CQLFragment> fieldsToVariables, StringBuilder sb) {
    boolean hasTtl = ttlSeconds != -1 || fieldsToVariables.containsValue(INTERNAL_TTL_VARNAME);
    boolean hasTimestamp =
        timestampMicros != -1 || fieldsToVariables.containsValue(INTERNAL_TIMESTAMP_VARNAME);
    if (hasTtl || hasTimestamp) {
      if (isCounterTable()) {
        throw new BulkConfigurationException(
            "Cannot set TTL or timestamp when updating a counter table.");
      }
      sb.append(" USING ");
      if (hasTtl) {
        sb.append("TTL ");
        if (ttlSeconds != -1) {
          sb.append(ttlSeconds);
        } else {
          sb.append(':');
          sb.append(INTERNAL_TTL_VARNAME);
        }
        if (hasTimestamp) {
          sb.append(" AND ");
        }
      }
      if (hasTimestamp) {
        sb.append("TIMESTAMP ");
        if (timestampMicros != -1) {
          sb.append(timestampMicros);
        } else {
          sb.append(':');
          sb.append(INTERNAL_TIMESTAMP_VARNAME);
        }
      }
    }
  }

  private String inferReadQuery(BiMap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("SELECT ");
    appendColumnNames(fieldsToVariables, sb, true);
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName);
    appendTokenRangeRestriction(sb);
    return sb.toString();
  }

  private void appendTokenRangeRestriction(StringBuilder sb) {
    sb.append(" WHERE ");
    appendTokenFunction(sb);
    sb.append(" > :start AND ");
    appendTokenFunction(sb);
    sb.append(" <= :end");
  }

  private String inferCountQuery(EnumSet<StatisticsMode> modes) {
    StringBuilder sb = new StringBuilder("SELECT ");
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    if (modes.contains(ranges) || modes.contains(hosts) || modes.contains(partitions)) {
      if (modes.contains(partitions)) {
        // we need to select the entire partition key, column by column
        Iterator<ColumnMetadata> it = partitionKey.iterator();
        while (it.hasNext()) {
          ColumnMetadata col = it.next();
          sb.append(quoteIfNecessary(col.getName()));
          if (it.hasNext()) {
            sb.append(',');
          }
        }
      } else {
        // we only need the row's token
        appendTokenFunction(sb);
      }
    } else {
      String selector = getGlobalCountSelector();
      sb.append(selector);
    }
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName).append(" WHERE ");
    appendTokenFunction(sb);
    sb.append(" > :start AND ");
    appendTokenFunction(sb);
    sb.append(" <= :end");
    return sb.toString();
  }

  @NotNull
  private String getGlobalCountSelector() {
    // When counting global rows we can select anything; we use the ttl of the first regular
    // column since it is an int and only takes 4 bytes; if no regular column exists, we use
    // the first partition key column.
    return table
        .getColumns()
        .stream()
        .filter(col -> !table.getPrimaryKey().contains(col))
        .map(col -> "TTL(" + quoteIfNecessary(col.getName()) + ")")
        .findFirst()
        .orElse(quoteIfNecessary(table.getPartitionKey().get(0).getName()));
  }

  private Set<CQLIdentifier> primaryKeyVariables() {
    Map<CQLIdentifier, CQLFragment> boundVariables = queryInspector.getAssignments();
    List<ColumnMetadata> primaryKeyColumns = table.getPrimaryKey();
    Set<CQLIdentifier> variables = new HashSet<>(primaryKeyColumns.size());
    for (ColumnMetadata column : primaryKeyColumns) {
      CQLFragment variable = boundVariables.get(CQLIdentifier.fromInternal(column.getName()));
      if (variable instanceof CQLIdentifier) {
        variables.add((CQLIdentifier) variable);
      }
    }
    return variables;
  }

  private void appendColumnNames(
      BiMap<MappingField, CQLFragment> fieldsToVariables,
      StringBuilder sb,
      boolean allowFunctions) {
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    while (it.hasNext()) {
      // this assumes that the variable name found in the mapping
      // corresponds to a CQL column having the exact same name.
      CQLFragment col = it.next();
      if (isPseudoColumn(col)) {
        // This is not a real column. Skip it.
        continue;
      }
      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      if (col instanceof FunctionCall && !allowFunctions) {
        throw new IllegalArgumentException(
            "Misplaced function call detected on the right side of a mapping entry; "
                + "please review your schema.mapping setting");
      }
      sb.append(col.asCql());
    }
  }

  private void appendTokenFunction(StringBuilder sb) {
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    sb.append("token(");
    Iterator<ColumnMetadata> pks = partitionKey.iterator();
    while (pks.hasNext()) {
      ColumnMetadata pk = pks.next();
      sb.append(quoteIfNecessary(pk.getName()));
      if (pks.hasNext()) {
        sb.append(',');
      }
    }
    sb.append(')');
  }

  private void appendPartitionKey(StringBuilder sb) {
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    Iterator<ColumnMetadata> pks = partitionKey.iterator();
    while (pks.hasNext()) {
      ColumnMetadata pk = pks.next();
      sb.append(quoteIfNecessary(pk.getName()));
      if (pks.hasNext()) {
        sb.append(',');
      }
    }
  }

  @NotNull
  private Set<CQLFragment> maybeSortCols(BiMap<MappingField, CQLFragment> fieldsToVariables) {
    Set<CQLFragment> cols;
    if (mapping != null && mapping.isIndexed()) {
      // order columns by index
      LinkedHashMap<MappingField, CQLFragment> sorted =
          MappingInspector.sortFieldsByIndex(fieldsToVariables);
      cols = new LinkedHashSet<>(sorted.values());
      cols.addAll(fieldsToVariables.values());
    } else {
      // preserve original order of variables in the mapping
      cols = new LinkedHashSet<>(fieldsToVariables.values());
    }
    return cols;
  }

  private static boolean isPseudoColumn(CQLFragment col) {
    return col == INTERNAL_TTL_VARNAME || col == INTERNAL_TIMESTAMP_VARNAME;
  }

  private static BiMap<MappingField, CQLFragment> processMappingFunctions(
      BiMap<MappingField, CQLFragment> fieldsToVariables, WorkflowType workflowType) {
    ImmutableBiMap.Builder<MappingField, CQLFragment> builder = ImmutableBiMap.builder();
    for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entrySet()) {
      if (entry.getValue() instanceof FunctionCall) {
        handleFunctionForUnload(builder, entry);
      } else if (entry.getKey() instanceof FunctionCall) {
        handleFunctionForLoad(workflowType);
      } else {
        builder.put(entry);
      }
    }
    return builder.build();
  }

  private static void handleFunctionForUnload(
      ImmutableBiMap.Builder<MappingField, CQLFragment> builder,
      Map.Entry<MappingField, CQLFragment> entry) {
    // functions as variables are are only allowed when unloading, but this has already
    // been validated when generating the query, so we don't need to re-validate here;
    // function calls when unloading should be included in the final mapping so that their
    // results can be retrieved by ReadResultMapper.
    builder.put(entry.getKey(), entry.getValue());
  }

  private static void handleFunctionForLoad(WorkflowType workflowType) {
    // functions as fields are only allowed when loading, and this needs to be validated now;
    // and we need to remove such function calls from the final mapping since RecordMapper
    // doesn't need them.
    if (workflowType != LOAD) {
      throw new IllegalArgumentException(
          "Misplaced function call detected on the left side of a mapping entry; "
              + "please review your schema.mapping setting");
    }
  }

  @NotNull
  private static Stream<TableMetadata> allVertexTables(KeyspaceMetadata keyspace) {
    return keyspace
        .getTables()
        .stream()
        .filter(tableMetadata -> tableMetadata.getVertexMetadata() != null);
  }

  @NotNull
  private static Stream<TableMetadata> allEdgeTables(KeyspaceMetadata keyspace) {
    return keyspace
        .getTables()
        .stream()
        .filter(tableMetadata -> tableMetadata.getEdgeMetadata() != null);
  }

  private static boolean hasGraphOptions(LoaderConfig config) {
    return config.hasPath(GRAPH)
        || config.hasPath(VERTEX)
        || config.hasPath(EDGE)
        || config.hasPath(FROM)
        || config.hasPath(TO);
  }

  private static boolean isGraph(KeyspaceMetadata keyspace) {
    return keyspace.getGraphEngine() != null && !keyspace.getGraphEngine().isEmpty();
  }

  private static boolean isSupportedGraph(KeyspaceMetadata keyspace) {
    return NATIVE.equals(keyspace.getGraphEngine());
  }
}
