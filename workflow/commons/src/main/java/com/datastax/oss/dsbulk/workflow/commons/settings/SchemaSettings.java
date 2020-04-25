/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import static com.datastax.oss.dsbulk.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.ALIASED_SELECTOR;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.INTERNAL;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.VARIABLE;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.INDEXED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_OR_INDEXED;
import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphKeyspaceMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLFragment;
import com.datastax.oss.dsbulk.mapping.CQLRenderMode;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.DefaultMapping;
import com.datastax.oss.dsbulk.mapping.FunctionCall;
import com.datastax.oss.dsbulk.mapping.IndexedMappingField;
import com.datastax.oss.dsbulk.mapping.MappedMappingField;
import com.datastax.oss.dsbulk.mapping.MappingField;
import com.datastax.oss.dsbulk.mapping.MappingInspector;
import com.datastax.oss.dsbulk.mapping.MappingPreference;
import com.datastax.oss.dsbulk.partitioner.TokenRangeReadStatementGenerator;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultRecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode;
import com.datastax.oss.dsbulk.workflow.commons.utils.WorkflowUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
  private static final String CORE = "Core";
  private static final String SPLITS = "splits";

  private final Config config;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private int splits;
  private MappingInspector mapping;
  private int ttlSeconds;
  private long timestampMicros;
  private RelationMetadata table;
  private KeyspaceMetadata keyspace;
  private CQLWord keyspaceName;
  private CQLWord tableName;
  private String query;
  private QueryInspector queryInspector;
  private PreparedStatement preparedStatement;
  private ImmutableSet<CQLFragment> writeTimeVariables;
  private MappingPreference mappingPreference;
  private ProtocolVersion protocolVersion;

  SchemaSettings(Config config) {
    this.config = config;
  }

  public void init(
      SchemaGenerationType schemaGenerationType,
      CqlSession session,
      boolean indexedMappingSupported,
      boolean mappedMappingSupported) {
    try {

      // Sanity Checks

      if (config.hasPath(KEYSPACE) && config.hasPath(GRAPH)) {
        throw new IllegalArgumentException(
            "Settings schema.keyspace and schema.graph are mutually exclusive");
      }
      if (config.hasPath(TABLE) && config.hasPath(VERTEX)) {
        throw new IllegalArgumentException(
            "Settings schema.table and schema.vertex are mutually exclusive");
      }
      if (config.hasPath(TABLE) && config.hasPath(EDGE)) {
        throw new IllegalArgumentException(
            "Settings schema.table and schema.edge are mutually exclusive");
      }
      if (config.hasPath(VERTEX) && config.hasPath(EDGE)) {
        throw new IllegalArgumentException(
            "Settings schema.vertex and schema.edge are mutually exclusive");
      }
      if (config.hasPath(EDGE)) {
        if (!config.hasPath(FROM)) {
          throw new IllegalArgumentException(
              "Setting schema.from is required when schema.edge is specified");
        }
        if (!config.hasPath(TO)) {
          throw new IllegalArgumentException(
              "Setting schema.to is required when schema.edge is specified");
        }
      }
      if (config.hasPath(QUERY)
          && (config.hasPath(TABLE) || config.hasPath(VERTEX) || config.hasPath(EDGE))) {
        throw new IllegalArgumentException(
            "Setting schema.query must not be defined if schema.table, schema.vertex or schema.edge are defined");
      }
      if ((!config.hasPath(KEYSPACE) && !config.hasPath(GRAPH))
          && (config.hasPath(TABLE) || config.hasPath(VERTEX) || config.hasPath(EDGE))) {
        throw new IllegalArgumentException(
            "Settings schema.keyspace or schema.graph must be defined if schema.table, schema.vertex or schema.edge are defined");
      }

      protocolVersion = session.getContext().getProtocolVersion();

      // Keyspace

      if (config.hasPath(KEYSPACE)) {
        keyspace = locateKeyspace(session.getMetadata(), config.getString(KEYSPACE));
      } else if (config.hasPath(GRAPH)) {
        keyspace = locateKeyspace(session.getMetadata(), config.getString(GRAPH));
      }

      // Table

      if (keyspace != null) {
        if (config.hasPath(TABLE)) {
          table = locateTable(keyspace, config.getString(TABLE), schemaGenerationType);
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
          throw new IllegalArgumentException(
              String.format(
                  "Expecting schema.queryTimestamp to be in ISO_ZONED_DATE_TIME format but got '%s'",
                  timestampStr));
        }
      } else {
        this.timestampMicros = -1L;
      }

      // Custom Query

      CQLWord usingTimestampVariable;
      CQLWord usingTTLVariable;

      if (config.hasPath(QUERY)) {

        query = config.getString(QUERY);
        queryInspector = new QueryInspector(query);

        if (queryInspector.getKeyspaceName().isPresent()) {
          if (keyspace != null) {
            throw new IllegalArgumentException(
                "Setting schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
          }
          CQLWord keyspaceName = queryInspector.getKeyspaceName().get();
          keyspace = session.getMetadata().getKeyspace(keyspaceName.asIdentifier()).orElse(null);
          if (keyspace == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Value for schema.query references a non-existent keyspace: %s",
                    keyspaceName.render(VARIABLE)));
          }
        } else if (keyspace == null) {
          throw new IllegalArgumentException(
              "Setting schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
        }

        CQLWord tableName = queryInspector.getTableName();
        table = keyspace.getTable(tableName.asIdentifier()).orElse(null);
        if (table == null) {
          table = keyspace.getView(tableName.asIdentifier()).orElse(null);
          if (table == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Value for schema.query references a non-existent table or materialized view: %s",
                    tableName.render(VARIABLE)));
          }
        }

        // If a query is provided, ttl and timestamp must not be.
        if (timestampMicros != -1 || ttlSeconds != -1) {
          throw new IllegalArgumentException(
              "Setting schema.query must not be defined if schema.queryTtl or schema.queryTimestamp is defined");
        }

        // If a query is provided, check now if it contains a USING TIMESTAMP variable,
        // or selectors containing a writetime() function call, and get their names.
        writeTimeVariables = queryInspector.getWriteTimeVariables();
        usingTimestampVariable = queryInspector.getUsingTimestampVariable().orElse(null);
        usingTTLVariable = queryInspector.getUsingTTLVariable().orElse(null);

      } else {

        writeTimeVariables = ImmutableSet.of();
        usingTimestampVariable = INTERNAL_TIMESTAMP_VARNAME;
        usingTTLVariable = INTERNAL_TTL_VARNAME;

        if (keyspace == null || table == null) {

          // Either the keyspace and table must be present, or the query must be present.
          throw new IllegalArgumentException(
              "When schema.query is not defined, "
                  + "then either schema.keyspace or schema.graph must be defined, "
                  + "and either schema.table, schema.vertex or schema.edge must be defined");
        }
      }

      assert keyspace != null;
      assert table != null;

      keyspaceName = CQLWord.fromCqlIdentifier(keyspace.getName());
      tableName = CQLWord.fromCqlIdentifier(table.getName());

      // Mapping

      if (indexedMappingSupported && mappedMappingSupported) {
        mappingPreference = MAPPED_OR_INDEXED;
      } else if (indexedMappingSupported) {
        mappingPreference = INDEXED_ONLY;
      } else if (mappedMappingSupported) {
        mappingPreference = MAPPED_ONLY;
      } else if (schemaGenerationType != SchemaGenerationType.READ_AND_COUNT) {
        throw new IllegalArgumentException(
            "Connector must support at least one of indexed or mapped mappings");
      }

      if (config.hasPath(MAPPING)) {

        if (schemaGenerationType == SchemaGenerationType.READ_AND_COUNT) {
          throw new IllegalArgumentException(
              "Setting schema.mapping must not be defined when counting rows in a table");
        }

        mapping =
            new MappingInspector(
                config.getString(MAPPING),
                schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE,
                mappingPreference,
                usingTimestampVariable,
                usingTTLVariable);

        Set<MappingField> fields = mapping.getExplicitVariables().keySet();
        Collection<CQLFragment> variables = mapping.getExplicitVariables().values();

        if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE
            && containsFunctionCalls(variables)) {
          // f1 = now() never allowed when loading
          throw new IllegalArgumentException(
              "Misplaced function call detected on the right side of a mapping entry; "
                  + "please review your schema.mapping setting");
        }
        if (schemaGenerationType == SchemaGenerationType.READ_AND_MAP
            && containsFunctionCalls(fields)) {
          // now() = c1 never allowed when unloading
          throw new IllegalArgumentException(
              "Misplaced function call detected on the left side of a mapping entry; "
                  + "please review your schema.mapping setting");
        }

        if (query != null) {
          if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE
              && containsFunctionCalls(fields)) {
            // now() = c1 only allowed if schema.query not present
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when loading if schema.mapping "
                    + "contains a function on the left side of a mapping entry");
          }
          if (schemaGenerationType == SchemaGenerationType.READ_AND_MAP
              && containsFunctionCalls(variables)) {
            // f1 = now() only allowed if schema.query not present
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when unloading if schema.mapping "
                    + "contains a function on the right side of a mapping entry");
          }
        } else {
          writeTimeVariables = mapping.getWriteTimeVariables();
        }
      } else {

        mapping =
            new MappingInspector(
                "*=*",
                schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE,
                mappingPreference,
                usingTimestampVariable,
                usingTTLVariable);
      }

      // Misc

      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      allowExtraFields = config.getBoolean(ALLOW_EXTRA_FIELDS);
      allowMissingFields = config.getBoolean(ALLOW_MISSING_FIELDS);
      splits = ConfigUtils.getThreads(config, SPLITS);

      // Final checks related to graph operations

      if (hasGraphOptions(config)) {

        WorkflowUtils.checkGraphCompatibility(session);

        if (!isGraph(keyspace)) {
          throw new IllegalStateException(
              "Graph operations requested but provided keyspace is not a graph: " + keyspaceName);
        }
        if (!isSupportedGraph(keyspace)) {
          throw new IllegalStateException(
              String.format(
                  "Graph operations requested but provided graph %s was created with an unsupported graph engine: %s",
                  keyspaceName, ((DseGraphKeyspaceMetadata) keyspace).getGraphEngine().get()));
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
          if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE) {
            LOGGER.warn(
                "Provided keyspace is a graph created with a legacy graph engine: "
                    + ((DseGraphKeyspaceMetadata) keyspace).getGraphEngine().get()
                    + "; attempting to load data into such a keyspace is not supported and "
                    + "may put the graph in an inconsistent state.");
          }
        }
      }

    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.schema");
    }
  }

  public RecordMapper createRecordMapper(
      CqlSession session, RecordMetadata recordMetadata, ConvertingCodecFactory codecFactory)
      throws IllegalArgumentException {
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(
            session,
            codecFactory,
            SchemaGenerationType.MAP_AND_WRITE,
            EnumSet.noneOf(StatisticsMode.class));
    if (protocolVersion.getCode() < DefaultProtocolVersion.V4.getCode() && nullToUnset) {
      LOGGER.warn(
          String.format(
              "Protocol version in use (%s) does not support unset bound variables; "
                  + "forcing schema.nullToUnset to false",
              protocolVersion));
      nullToUnset = false;
    }
    return new DefaultRecordMapper(
        preparedStatement,
        mutatesOnlyStaticColumns() ? partitionKeyVariables() : primaryKeyVariables(),
        protocolVersion,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields);
  }

  public ReadResultMapper createReadResultMapper(
      CqlSession session, RecordMetadata recordMetadata, ConvertingCodecFactory codecFactory)
      throws IllegalArgumentException {
    // we don't check that mapping records are supported when unloading, the only thing that matters
    // is the order in which fields appear in the record.
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(
            session,
            codecFactory,
            SchemaGenerationType.READ_AND_MAP,
            EnumSet.noneOf(StatisticsMode.class));
    return new DefaultReadResultMapper(mapping, recordMetadata);
  }

  public ReadResultCounter createReadResultCounter(
      CqlSession session,
      ConvertingCodecFactory codecFactory,
      EnumSet<StatsSettings.StatisticsMode> modes,
      int numPartitions) {
    prepareStatementAndCreateMapping(session, null, SchemaGenerationType.READ_AND_COUNT, modes);
    ProtocolVersion protocolVersion = session.getContext().getProtocolVersion();
    Metadata metadata = session.getMetadata();
    if (modes.contains(StatisticsMode.partitions) && table.getClusteringColumns().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot count partitions for table %s: it has no clustering column.",
              tableName.render(VARIABLE)));
    }
    return new DefaultReadResultCounter(
        keyspace.getName(), metadata, modes, numPartitions, protocolVersion, codecFactory);
  }

  public List<Statement<?>> createReadStatements(CqlSession session) {
    ColumnDefinitions variables = preparedStatement.getVariableDefinitions();
    if (variables.size() == 0) {
      return Collections.singletonList(preparedStatement.bind());
    }
    boolean ok = true;
    Optional<CQLWord> start = queryInspector.getTokenRangeRestrictionStartVariable();
    Optional<CQLWord> end = queryInspector.getTokenRangeRestrictionEndVariable();
    if (!start.isPresent() || !end.isPresent()) {
      ok = false;
    }
    if (start.isPresent() && end.isPresent()) {
      Optional<CQLWord> unrecognized =
          StreamSupport.stream(variables.spliterator(), false)
              .map(columnDefinition -> columnDefinition.getName().asInternal())
              .map(CQLWord::fromInternal)
              .filter(name -> !name.equals(start.get()) && !name.equals(end.get()))
              .findAny();
      ok = !unrecognized.isPresent();
    }
    if (!ok) {
      throw new IllegalArgumentException(
          "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
              + "the WHERE clause is only allowed to contain one token range restriction "
              + "of the form: WHERE token(...) > ? AND token(...) <= ?");
    }
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap =
        metadata
            .getTokenMap()
            .orElseThrow(() -> new IllegalStateException("Token metadata not present"));
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    List<Statement<?>> statements =
        generator.generate(
            splits,
            range -> {
              BoundStatement bs = preparedStatement.bind();
              int startIdx = queryInspector.getTokenRangeRestrictionStartVariableIndex();
              Token startToken = tokenMap.parse(range.start().toString());
              bs = bs.setToken(startIdx, startToken);
              int endIdx = queryInspector.getTokenRangeRestrictionEndVariableIndex();
              Token endToken = tokenMap.parse(range.end().toString());
              bs = bs.setToken(endIdx, endToken);
              return bs;
            });
    LOGGER.debug("Generated {} bound statements", statements.size());
    return statements;
  }

  @NonNull
  public RowType getRowType() {
    boolean isTable = table instanceof DseTableMetadata;
    DseGraphTableMetadata mtable = (isTable ? (DseGraphTableMetadata) table : null);
    if (isTable && mtable.getVertex().isPresent()) {
      return RowType.VERTEX;
    } else if (isTable && mtable.getEdge().isPresent()) {
      return RowType.EDGE;
    } else {
      return RowType.REGULAR;
    }
  }

  public boolean isAllowExtraFields() {
    return allowExtraFields;
  }

  public boolean isAllowMissingFields() {
    return allowMissingFields;
  }

  public boolean isSearchQuery() {
    return queryInspector.hasSearchClause();
  }

  @NonNull
  private DefaultMapping prepareStatementAndCreateMapping(
      CqlSession session,
      ConvertingCodecFactory codecFactory,
      SchemaGenerationType schemaGenerationType,
      EnumSet<StatsSettings.StatisticsMode> modes) {
    ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables = null;
    if (!config.hasPath(QUERY)) {
      // in the absence of user-provided queries, create the mapping *before* query generation and
      // preparation
      fieldsToVariables =
          createFieldsToVariablesMap(
              table.getColumns().values().stream()
                  .filter(col -> !isDSESearchPseudoColumn(col))
                  .map(columnMetadata -> columnMetadata.getName().asInternal())
                  .map(CQLWord::fromInternal)
                  .collect(Collectors.toList()));
      // query generation
      if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE) {
        if (isCounterTable()) {
          query = inferUpdateCounterQuery(fieldsToVariables);
        } else {
          query = inferInsertQuery(fieldsToVariables);
        }
      } else if (schemaGenerationType == SchemaGenerationType.READ_AND_MAP) {
        query = inferReadQuery(fieldsToVariables);
      } else if (schemaGenerationType == SchemaGenerationType.READ_AND_COUNT) {
        query = inferCountQuery(modes);
      }
      LOGGER.debug("Inferred query: {}", query);
      queryInspector = new QueryInspector(query);
      // validate generated query
      if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
      fieldsToVariables = processMappingFunctions(fieldsToVariables);
    }
    assert query != null;
    assert queryInspector != null;
    if (!queryInspector.getKeyspaceName().isPresent()) {
      session.execute("USE " + keyspaceName);
    }
    // Transform user-provided queries before preparation
    if (config.hasPath(QUERY)) {
      if ((schemaGenerationType == SchemaGenerationType.READ_AND_MAP
              || schemaGenerationType == SchemaGenerationType.READ_AND_COUNT)
          && !queryInspector.hasWhereClause()) {
        int whereClauseIndex = queryInspector.getFromClauseEndIndex() + 1;
        StringBuilder sb = new StringBuilder(query.substring(0, whereClauseIndex));
        appendTokenRangeRestriction(sb);
        query = sb.append(query.substring(whereClauseIndex)).toString();
      }
      if (schemaGenerationType == SchemaGenerationType.READ_AND_COUNT) {
        if (modes.contains(StatisticsMode.partitions)
            || modes.contains(StatisticsMode.ranges)
            || modes.contains(StatisticsMode.hosts)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot count with stats.modes = %s when schema.query is provided; "
                      + "only stats.modes = [global] is allowed",
                  modes));
        }
        if (queryInspector.isSelectStar()) {
          StringBuilder sb = new StringBuilder("SELECT ");
          // reduce row size by only selecting one column instead of all columns
          sb.append(getGlobalCountSelector());
          query =
              sb.append(' ')
                  .append(query.substring(queryInspector.getFromClauseStartIndex()))
                  .toString();
        }
      }
      queryInspector = new QueryInspector(query);
    }
    preparedStatement = session.prepare(query);
    if (config.hasPath(QUERY)) {
      // in the presence of user-provided queries, create the mapping *after* query preparation
      ColumnDefinitions variables = getVariables(schemaGenerationType);
      fieldsToVariables =
          createFieldsToVariablesMap(
              StreamSupport.stream(variables.spliterator(), false)
                  .map(columnDefinition -> columnDefinition.getName().asInternal())
                  .map(CQLWord::fromInternal)
                  .collect(Collectors.toList()));
      // validate user-provided query
      if (schemaGenerationType == SchemaGenerationType.MAP_AND_WRITE) {
        if (mutatesOnlyStaticColumns()) {
          // DAT-414: mutations that only affect static columns are allowed
          // to skip the clustering columns, only the partition key should be present.
          validatePartitionKeyPresent(fieldsToVariables);
        } else {
          validatePrimaryKeyPresent(fieldsToVariables);
        }
      }
    }
    assert fieldsToVariables != null;
    return new DefaultMapping(
        processFieldsToVariables(fieldsToVariables),
        codecFactory,
        processWriteTimeVariables(writeTimeVariables));
  }

  private boolean isDSESearchPseudoColumn(ColumnMetadata col) {
    return col.getName().asInternal().equals("solr_query")
        && col.getType() == DataTypes.TEXT
        && tableHasDSESearchIndex();
  }

  private boolean tableHasDSESearchIndex() {
    if (table instanceof TableMetadata) {
      for (IndexMetadata index : ((TableMetadata) table).getIndexes().values()) {
        if ("com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
            .equals(index.getClassName().orElse(null))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isCounterTable() {
    return table.getColumns().values().stream()
        .anyMatch(c -> c.getType().equals(DataTypes.COUNTER));
  }

  private ColumnDefinitions getVariables(SchemaGenerationType schemaGenerationType) {
    switch (schemaGenerationType) {
      case MAP_AND_WRITE:
        return preparedStatement.getVariableDefinitions();
      case READ_AND_MAP:
      case READ_AND_COUNT:
        return preparedStatement.getResultSetDefinitions();
      default:
        throw new AssertionError();
    }
  }

  private ImmutableMultimap<MappingField, CQLFragment> createFieldsToVariablesMap(
      Collection<CQLFragment> columns) throws IllegalArgumentException {
    ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables;
    if (mapping.isInferring()) {
      fieldsToVariables = inferFieldsToVariablesMap(columns);
    } else {
      fieldsToVariables = ImmutableMultimap.of();
    }

    ImmutableMultimap<MappingField, CQLFragment> explicitVariables = mapping.getExplicitVariables();
    if (!explicitVariables.isEmpty()) {
      ImmutableMultimap.Builder<MappingField, CQLFragment> builder = ImmutableMultimap.builder();
      for (Map.Entry<MappingField, CQLFragment> entry : explicitVariables.entries()) {
        builder.put(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
        if (!explicitVariables.containsKey(entry.getKey())
            && !explicitVariables.containsValue(entry.getValue())) {
          builder.put(entry.getKey(), entry.getValue());
        }
      }
      fieldsToVariables = builder.build();
    }

    Preconditions.checkState(
        !fieldsToVariables.isEmpty(),
        "Mapping was absent and could not be inferred, please provide an explicit mapping");

    validateAllFieldsPresent(fieldsToVariables, columns);

    return fieldsToVariables;
  }

  private KeyspaceMetadata locateKeyspace(Metadata metadata, String keyspaceNameInternal) {
    CqlIdentifier keyspaceName = CqlIdentifier.fromInternal(keyspaceNameInternal);
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName).orElse(null);
    if (keyspace == null) {
      Optional<KeyspaceMetadata> match =
          metadata.getKeyspaces().values().stream()
              .filter(k -> k.getName().asInternal().equalsIgnoreCase(keyspaceNameInternal))
              .findFirst();
      if (match.isPresent()) {
        String similarName = match.get().getName().asCql(true);
        throw new IllegalArgumentException(
            String.format(
                "Keyspace %s does not exist, however a keyspace %s was found. Did you mean to use -k %s?",
                keyspaceName.asCql(true), similarName, similarName));
      } else {
        throw new IllegalArgumentException(
            String.format("Keyspace %s does not exist", keyspaceName.asCql(true)));
      }
    }
    return keyspace;
  }

  @NonNull
  private RelationMetadata locateTable(
      KeyspaceMetadata keyspace,
      String tableNameInternal,
      SchemaGenerationType schemaGenerationType) {
    CqlIdentifier tableName = CqlIdentifier.fromInternal(tableNameInternal);
    RelationMetadata table = keyspace.getTable(tableName).orElse(null);
    if (table == null) {
      if (schemaGenerationType == SchemaGenerationType.READ_AND_COUNT
          || schemaGenerationType == SchemaGenerationType.READ_AND_MAP) {
        table = keyspace.getView(tableName).orElse(null);
        if (table == null) {
          Optional<ViewMetadata> match =
              keyspace.getViews().values().stream()
                  .filter(t -> t.getName().asInternal().equalsIgnoreCase(tableNameInternal))
                  .findFirst();
          if (match.isPresent()) {
            String similarName = match.get().getName().asCql(true);
            throw new IllegalArgumentException(
                String.format(
                    "Table or materialized view %s does not exist, "
                        + "however a materialized view %s was found. Did you mean to use -t %s?",
                    tableName.asCql(true), similarName, similarName));
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Table or materialized view %s does not exist", tableName.asCql(true)));
          }
        }
      } else {
        Optional<TableMetadata> match =
            keyspace.getTables().values().stream()
                .filter(t -> t.getName().asInternal().equalsIgnoreCase(tableNameInternal))
                .findFirst();
        if (match.isPresent()) {
          String similarName = match.get().getName().asCql(true);
          throw new IllegalArgumentException(
              String.format(
                  "Table %s does not exist, however a table %s was found. Did you mean to use -t %s?",
                  tableName.asCql(true), similarName, similarName));
        } else {
          throw new IllegalArgumentException(
              String.format("Table %s does not exist", tableName.asCql(true)));
        }
      }
    }
    return table;
  }

  @NonNull
  private TableMetadata locateVertexTable(KeyspaceMetadata keyspace, String vertexLabelInternal) {
    CqlIdentifier vertexLabel = CqlIdentifier.fromInternal(vertexLabelInternal);
    Optional<DseGraphTableMetadata> vertex =
        allVertexTables(keyspace)
            .filter(table -> table.getVertex().get().getLabelName().equals(vertexLabel))
            .findFirst();
    if (!vertex.isPresent()) {
      Optional<? extends DseVertexMetadata> match =
          allVertexTables(keyspace)
              .filter(
                  table ->
                      table
                          .getVertex()
                          .get()
                          .getLabelName()
                          .asInternal()
                          .equalsIgnoreCase(vertexLabelInternal))
              .map(t -> t.getVertex().get())
              .findFirst();
      if (match.isPresent()) {
        String similarName = match.get().getLabelName().asCql(true);
        throw new IllegalArgumentException(
            String.format(
                "Vertex label %s does not exist, however a vertex label %s was found. Did you mean to use -v %s?",
                vertexLabel.asCql(true), similarName, similarName));
      } else {
        throw new IllegalArgumentException(
            String.format("Vertex label %s does not exist", vertexLabel.asCql(true)));
      }
    }
    return vertex.get();
  }

  @NonNull
  private TableMetadata locateEdgeTable(
      KeyspaceMetadata keyspace,
      String edgeLabelInternal,
      String fromVertexInternal,
      String toVertexInternal) {
    CqlIdentifier edgeLabel = CqlIdentifier.fromInternal(edgeLabelInternal);
    CqlIdentifier fromVertex = CqlIdentifier.fromInternal(fromVertexInternal);
    CqlIdentifier toVertex = CqlIdentifier.fromInternal(toVertexInternal);
    Optional<DseGraphTableMetadata> edge =
        allEdgeTables(keyspace)
            .filter(table -> table.getEdge().get().getLabelName().equals(edgeLabel))
            .filter(table -> table.getEdge().get().getFromLabel().equals(fromVertex))
            .filter(table -> table.getEdge().get().getToLabel().equals(toVertex))
            .findFirst();
    if (!edge.isPresent()) {
      Optional<? extends DseEdgeMetadata> match =
          allEdgeTables(keyspace)
              .map(DseGraphTableMetadata::getEdge)
              .filter(e -> e.get().getLabelName().asInternal().equalsIgnoreCase(edgeLabelInternal))
              .filter(e -> e.get().getFromLabel().asInternal().equalsIgnoreCase(fromVertexInternal))
              .filter(e -> e.get().getToLabel().asInternal().equalsIgnoreCase(toVertexInternal))
              .map(Optional::get)
              .findFirst();
      if (match.isPresent()) {
        DseEdgeMetadata edgeMetadata = match.get();
        String similarLabel = edgeMetadata.getLabelName().asCql(true);
        String similarFrom = edgeMetadata.getFromLabel().asCql(true);
        String similarTo = edgeMetadata.getToLabel().asCql(true);
        throw new IllegalArgumentException(
            String.format(
                "Edge label %s from %s to %s does not exist, "
                    + "however an edge label %s from %s to %s was found. "
                    + "Did you mean to use -e %s -from %s -to %s?",
                edgeLabel.asCql(true),
                fromVertex.asCql(true),
                toVertex.asCql(true),
                similarLabel,
                similarFrom,
                similarTo,
                similarLabel,
                similarFrom,
                similarTo));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Edge label %s from %s to %s does not exist",
                edgeLabel.asCql(true), fromVertex.asCql(true), toVertex.asCql(true)));
      }
    }
    return edge.get();
  }

  @NonNull
  private static Stream<DseGraphTableMetadata> allVertexTables(KeyspaceMetadata keyspace) {
    return keyspace.getTables().values().stream()
        .filter(DseGraphTableMetadata.class::isInstance)
        .map(DseGraphTableMetadata.class::cast)
        .filter(dseTableMetadata -> dseTableMetadata.getVertex().isPresent());
  }

  @NonNull
  private static Stream<DseGraphTableMetadata> allEdgeTables(KeyspaceMetadata keyspace) {
    return keyspace.getTables().values().stream()
        .filter(DseGraphTableMetadata.class::isInstance)
        .map(DseGraphTableMetadata.class::cast)
        .filter(dseTableMetadata -> dseTableMetadata.getEdge().isPresent());
  }

  private void validateAllFieldsPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables,
      Collection<CQLFragment> columns) {
    fieldsToVariables.forEach(
        (key, value) -> {
          if (value instanceof CQLWord && !isPseudoColumn(value) && !columns.contains(value)) {
            if (!config.hasPath(QUERY)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Schema mapping entry %s doesn't match any column found in table %s",
                      value.render(VARIABLE), tableName.render(VARIABLE)));
            } else {
              assert query != null;
              throw new IllegalArgumentException(
                  String.format(
                      "Schema mapping entry %s doesn't match any bound variable found in query: '%s'",
                      value.render(VARIABLE), query));
            }
          }
        });
  }

  private boolean mutatesOnlyStaticColumns() {
    // this method should only be called for mutating queries
    assert !queryInspector.getAssignments().isEmpty();
    for (CQLWord column : queryInspector.getAssignments().keySet()) {
      ColumnMetadata col =
          table
              .getColumn(column.asIdentifier())
              .orElseThrow(() -> new IllegalStateException("Column does not exist: " + column));
      if (table.getPartitionKey().contains(col)) {
        // partition key should always be present
        continue;
      }
      if (!col.isStatic()) {
        return false;
      }
    }
    return true;
  }

  private void validatePrimaryKeyPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    validateKeyPresent(fieldsToVariables, table.getPrimaryKey());
  }

  private void validatePartitionKeyPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    validateKeyPresent(fieldsToVariables, table.getPartitionKey());
  }

  private void validateKeyPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables,
      List<ColumnMetadata> columns) {
    Collection<CQLFragment> mappingVariables = fieldsToVariables.values();
    Map<CQLWord, CQLFragment> queryVariables = queryInspector.getAssignments();
    for (ColumnMetadata pk : columns) {
      CQLWord pkVariable = CQLWord.fromInternal(pk.getName().asInternal());
      CQLFragment queryVariable = queryVariables.get(pkVariable);
      // the provided query did not contain such column
      if (queryVariable == null) {
        throw new IllegalArgumentException(
            "Missing required primary key column "
                + pkVariable.render(VARIABLE)
                + " from schema.mapping or schema.query");
      }
      // do not check if the mapping contains a PK
      // if the PK is mapped to a function or a literal in the query (DAT-326)
      if (queryVariable instanceof CQLWord) {
        // the mapping did not contain such column
        if (!mappingVariables.contains(queryVariable)) {
          throw new IllegalArgumentException(
              "Missing required primary key column "
                  + pkVariable.render(VARIABLE)
                  + " from schema.mapping");
        }
      }
    }
  }

  private ImmutableMultimap<MappingField, CQLFragment> inferFieldsToVariablesMap(
      Collection<CQLFragment> columns) {

    // use a builder to preserve iteration order
    ImmutableMultimap.Builder<MappingField, CQLFragment> fieldsToVariables =
        ImmutableMultimap.builder();

    int i = 0;
    for (CQLFragment colName : columns) {
      if (!mapping.getExcludedVariables().contains(colName)) {
        if (mappingPreference == INDEXED_ONLY) {
          fieldsToVariables.put(new IndexedMappingField(i), colName);
        } else {
          fieldsToVariables.put(new MappedMappingField(colName.render(INTERNAL)), colName);
        }
      }
      i++;
    }
    return fieldsToVariables.build();
  }

  private String inferInsertQuery(ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName.render(VARIABLE))
        .append('.')
        .append(tableName.render(VARIABLE))
        .append(" (");
    appendColumnNames(fieldsToVariables, sb, VARIABLE);
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
        sb.append(", ");
      }
      isFirst = false;
      // for insert queries there can be only one field mapped to a given column
      MappingField field = fieldsToVariables.inverse().get(col).iterator().next();
      if (field instanceof FunctionCall) {
        // append the function call as is
        sb.append(((FunctionCall) field).render(NAMED_ASSIGNMENT));
      } else {
        sb.append(col.render(NAMED_ASSIGNMENT));
      }
    }
    sb.append(')');
    addTimestampAndTTL(sb);
    return sb.toString();
  }

  private String inferUpdateCounterQuery(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(keyspaceName.render(VARIABLE)).append('.').append(tableName.render(VARIABLE));
    // Note: TTL and timestamp are not allowed in counter queries;
    // a test is made inside the following method
    addTimestampAndTTL(sb);
    sb.append(" SET ");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    List<CQLFragment> pks =
        table.getPrimaryKey().stream()
            .map(columnMetadata -> columnMetadata.getName().asInternal())
            .map(CQLWord::fromInternal)
            .collect(Collectors.toList());
    while (it.hasNext()) {
      CQLFragment col = it.next();
      if (pks.contains(col)) {
        continue;
      }
      // for update queries there can be only one field mapped to a given column
      MappingField field = fieldsToVariables.inverse().get(col).iterator().next();
      if (field instanceof FunctionCall) {
        throw new IllegalArgumentException(
            "Function calls are not allowed when updating a counter table.");
      }
      if (!isFirst) {
        sb.append(", ");
      }
      isFirst = false;
      sb.append(col.render(VARIABLE))
          .append(" = ")
          .append(col.render(VARIABLE))
          .append(" + ")
          .append(col.render(NAMED_ASSIGNMENT));
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
      sb.append(col.render(VARIABLE)).append(" = ").append(col.render(NAMED_ASSIGNMENT));
    }
    return sb.toString();
  }

  private void addTimestampAndTTL(StringBuilder sb) {
    boolean hasTtl = ttlSeconds != -1 || (mapping != null && mapping.hasUsingTTL());
    boolean hasTimestamp =
        timestampMicros != -1 || (mapping != null && mapping.hasUsingTimestamp());
    if (hasTtl || hasTimestamp) {
      if (isCounterTable()) {
        throw new IllegalArgumentException(
            "Cannot set TTL or timestamp when updating a counter table.");
      }
      sb.append(" USING ");
      if (hasTtl) {
        sb.append("TTL ");
        if (ttlSeconds != -1) {
          sb.append(ttlSeconds);
        } else {
          sb.append(INTERNAL_TTL_VARNAME.render(NAMED_ASSIGNMENT));
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
          sb.append(INTERNAL_TIMESTAMP_VARNAME.render(NAMED_ASSIGNMENT));
        }
      }
    }
  }

  private String inferReadQuery(ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("SELECT ");
    appendColumnNames(fieldsToVariables, sb, ALIASED_SELECTOR);
    sb.append(" FROM ")
        .append(keyspaceName.render(VARIABLE))
        .append('.')
        .append(tableName.render(VARIABLE));
    appendTokenRangeRestriction(sb);
    return sb.toString();
  }

  private void appendTokenRangeRestriction(StringBuilder sb) {
    sb.append(" WHERE ");
    appendTokenFunction(sb);
    sb.append(" > ");
    sb.append(":start");
    sb.append(" AND ");
    appendTokenFunction(sb);
    sb.append(" <= ");
    sb.append(":end");
  }

  private String inferCountQuery(EnumSet<StatisticsMode> modes) {
    StringBuilder sb = new StringBuilder("SELECT ");
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    if (modes.contains(StatisticsMode.ranges)
        || modes.contains(StatisticsMode.hosts)
        || modes.contains(StatisticsMode.partitions)) {
      if (modes.contains(StatisticsMode.partitions)) {
        // we need to select the entire partition key, column by column
        Iterator<ColumnMetadata> it = partitionKey.iterator();
        while (it.hasNext()) {
          ColumnMetadata col = it.next();
          sb.append(col.getName().asCql(true));
          if (it.hasNext()) {
            sb.append(", ");
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
    sb.append(" FROM ")
        .append(keyspaceName.render(VARIABLE))
        .append('.')
        .append(tableName.render(VARIABLE));
    appendTokenRangeRestriction(sb);
    return sb.toString();
  }

  @NonNull
  private String getGlobalCountSelector() {
    // When counting global rows we can select anything; we use the first partition key column.
    return table.getPartitionKey().get(0).getName().asCql(true);
  }

  private Set<CQLWord> primaryKeyVariables() {
    return columnsToVariables(table.getPrimaryKey());
  }

  private Set<CQLWord> partitionKeyVariables() {
    return columnsToVariables(table.getPartitionKey());
  }

  private Set<CQLWord> columnsToVariables(List<ColumnMetadata> columns) {
    Map<CQLWord, CQLFragment> boundVariables = queryInspector.getAssignments();
    Set<CQLWord> variables = new HashSet<>(columns.size());
    for (ColumnMetadata column : columns) {
      CQLFragment variable =
          boundVariables.get(CQLWord.fromInternal(column.getName().asInternal()));
      if (variable instanceof CQLWord) {
        variables.add((CQLWord) variable);
      }
    }
    return variables;
  }

  private void appendColumnNames(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables,
      StringBuilder sb,
      CQLRenderMode mode) {
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
        sb.append(", ");
      }
      isFirst = false;
      sb.append(col.render(mode));
    }
  }

  private void appendTokenFunction(StringBuilder sb) {
    List<? extends ColumnMetadata> partitionKey = table.getPartitionKey();
    sb.append("token(");
    Iterator<? extends ColumnMetadata> pks = partitionKey.iterator();
    while (pks.hasNext()) {
      ColumnMetadata pk = pks.next();
      sb.append(pk.getName().asCql(true));
      if (pks.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append(')');
  }

  @NonNull
  private Set<CQLFragment> maybeSortCols(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    Set<CQLFragment> cols;
    if (mappingPreference == INDEXED_ONLY) {
      // order columns by index
      Multimap<MappingField, CQLFragment> sorted =
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

  @NonNull
  private static ImmutableMultimap<MappingField, CQLFragment> processMappingFunctions(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    ImmutableMultimap.Builder<MappingField, CQLFragment> builder = ImmutableMultimap.builder();
    for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      if (entry.getKey() instanceof FunctionCall) {
        // functions as fields should be included in the final mapping arg by arg, for every arg
        // that is a field identifier, e.g. plus(fieldA,fieldB) will generate two bound variables:
        // INSERT INTO ... VALUES ( plus(:fieldA,:fieldB) )
        for (CQLFragment arg : ((FunctionCall) entry.getKey()).getArgs()) {
          if (arg instanceof CQLWord) {
            // for each arg, create an arg -> arg mapping
            builder.put(new MappedMappingField(arg.render(INTERNAL)), arg);
          }
        }
      } else {
        // functions as variables must be included in the final mapping as a whole, e.g.
        // plus(col1,col2) will generate one result set variable:
        // SELECT plus(col1,col2) AS "plus(col1,col2)" ...
        builder.put(entry);
      }
    }
    return builder.build();
  }

  @NonNull
  private static ImmutableSetMultimap<Field, CQLWord> processFieldsToVariables(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    ImmutableSetMultimap.Builder<Field, CQLWord> builder = ImmutableSetMultimap.builder();
    for (Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      // transform all CQL fragments into CQL identifiers since that's the way they will be searched
      // for in DefaultMapping
      CQLFragment value = entry.getValue();
      String internal = value.render(INTERNAL);
      builder.put(entry.getKey(), CQLWord.fromInternal(internal));
    }
    return builder.build();
  }

  @NonNull
  private static ImmutableSet<CQLWord> processWriteTimeVariables(
      ImmutableSet<CQLFragment> writeTimeVariables) {
    ImmutableSet.Builder<CQLWord> builder = ImmutableSet.builder();
    for (CQLFragment variable : writeTimeVariables) {
      // transform all CQL fragments into CQL identifiers since that's the way they will be searched
      // for in DefaultMapping
      String internal = variable.render(INTERNAL);
      builder.add(CQLWord.fromInternal(internal));
    }
    return builder.build();
  }

  private static boolean containsFunctionCalls(Collection<?> coll) {
    return coll.stream().anyMatch(FunctionCall.class::isInstance);
  }

  private static boolean hasGraphOptions(Config config) {
    return config.hasPath(GRAPH)
        || config.hasPath(VERTEX)
        || config.hasPath(EDGE)
        || config.hasPath(FROM)
        || config.hasPath(TO);
  }

  private static boolean isGraph(KeyspaceMetadata keyspace) {
    return (keyspace instanceof DseGraphKeyspaceMetadata)
        && ((DseGraphKeyspaceMetadata) keyspace).getGraphEngine().isPresent();
  }

  private static boolean isSupportedGraph(KeyspaceMetadata keyspace) {
    return (keyspace instanceof DseGraphKeyspaceMetadata)
        && ((DseGraphKeyspaceMetadata) keyspace)
            .getGraphEngine()
            .filter(e -> e.equals(CORE))
            .isPresent();
  }
}
