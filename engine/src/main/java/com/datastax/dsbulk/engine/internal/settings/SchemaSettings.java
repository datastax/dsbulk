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
import static com.datastax.driver.core.ProtocolVersion.V1;
import static com.datastax.dsbulk.engine.WorkflowType.COUNT;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.ALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.INTERNAL;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.POSITIONAL_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.UNALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.INDEXED_ONLY;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.MAPPED_ONLY;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.MAPPED_OR_INDEXED;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.partitioner.TokenRangeReadStatement;
import com.datastax.dsbulk.commons.partitioner.TokenRangeReadStatementGenerator;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.CQLFragment;
import com.datastax.dsbulk.engine.internal.schema.CQLIdentifier;
import com.datastax.dsbulk.engine.internal.schema.CQLRenderMode;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.FunctionCall;
import com.datastax.dsbulk.engine.internal.schema.IndexedMappingField;
import com.datastax.dsbulk.engine.internal.schema.MappedMappingField;
import com.datastax.dsbulk.engine.internal.schema.MappingField;
import com.datastax.dsbulk.engine.internal.schema.MappingInspector;
import com.datastax.dsbulk.engine.internal.schema.MappingPreference;
import com.datastax.dsbulk.engine.internal.schema.QueryInspector;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.typesafe.config.ConfigException;
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
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSettings.class);

  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String KEYSPACE = "keyspace";
  private static final String TABLE = "table";
  private static final String MAPPING = "mapping";
  private static final String ALLOW_EXTRA_FIELDS = "allowExtraFields";
  private static final String ALLOW_MISSING_FIELDS = "allowMissingFields";
  private static final String QUERY = "query";
  private static final String QUERY_TTL = "queryTtl";
  private static final String QUERY_TIMESTAMP = "queryTimestamp";
  private static final String SPLITS = "splits";

  private final LoaderConfig config;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private int splits;
  private MappingInspector mapping;
  private int ttlSeconds;
  private long timestampMicros;
  private AbstractTableMetadata table;
  private KeyspaceMetadata keyspace;
  private String keyspaceName;
  private String tableName;
  private String query;
  private QueryInspector queryInspector;
  private PreparedStatement preparedStatement;
  private ImmutableSet<CQLFragment> writeTimeVariables;
  private MappingPreference mappingPreference;
  private ProtocolVersion protocolVersion;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init(
      WorkflowType workflowType,
      Cluster cluster,
      boolean indexedMappingSupported,
      boolean mappedMappingSupported) {
    try {

      // Sanity Checks

      if (config.hasPath(QUERY) && config.hasPath(TABLE)) {
        throw new BulkConfigurationException(
            "Setting schema.query must not be defined if schema.table is defined");
      }
      if (!config.hasPath(KEYSPACE) && config.hasPath(TABLE)) {
        throw new BulkConfigurationException(
            "Setting schema.keyspace must be defined if schema.table is defined");
      }

      protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

      // Keyspace

      if (config.hasPath(KEYSPACE)) {
        keyspace = locateKeyspace(cluster.getMetadata(), config.getString(KEYSPACE));
      }

      // Table

      if (keyspace != null) {
        if (config.hasPath(TABLE)) {
          table = locateTable(keyspace, config.getString(TABLE), workflowType);
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

      CQLIdentifier usingTimestampVariable;
      CQLIdentifier usingTTLVariable;

      if (config.hasPath(QUERY)) {

        query = config.getString(QUERY);
        queryInspector = new QueryInspector(query);

        if (queryInspector.getKeyspaceName().isPresent()) {
          if (keyspace != null) {
            throw new BulkConfigurationException(
                "Setting schema.keyspace must not be provided when schema.query contains a keyspace-qualified statement");
          }
          CQLIdentifier keyspaceName = queryInspector.getKeyspaceName().get();
          keyspace = cluster.getMetadata().getKeyspace(keyspaceName.render(VARIABLE));
          if (keyspace == null) {
            throw new BulkConfigurationException(
                String.format(
                    "Value for schema.query references a non-existent keyspace: %s",
                    keyspaceName.render(VARIABLE)));
          }
        } else if (keyspace == null) {
          throw new BulkConfigurationException(
              "Setting schema.keyspace must be provided when schema.query does not contain a keyspace-qualified statement");
        }

        CQLIdentifier tableName = queryInspector.getTableName();
        table = keyspace.getTable(tableName.render(VARIABLE));
        if (table == null) {
          table = keyspace.getMaterializedView(tableName.render(VARIABLE));
          if (table == null) {
            throw new BulkConfigurationException(
                String.format(
                    "Value for schema.query references a non-existent table or materialized view: %s",
                    tableName.render(VARIABLE)));
          }
        }

        // If a query is provided, ttl and timestamp must not be.
        if (timestampMicros != -1 || ttlSeconds != -1) {
          throw new BulkConfigurationException(
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
          throw new BulkConfigurationException(
              "When schema.query is not defined, then schema.keyspace and schema.table must be defined");
        }
      }

      assert keyspace != null;
      assert table != null;

      keyspaceName = quoteIfNecessary(keyspace.getName());
      tableName = quoteIfNecessary(table.getName());

      // Mapping

      if (indexedMappingSupported && mappedMappingSupported) {
        mappingPreference = MAPPED_OR_INDEXED;
      } else if (indexedMappingSupported) {
        mappingPreference = INDEXED_ONLY;
      } else if (mappedMappingSupported) {
        mappingPreference = MAPPED_ONLY;
      } else {
        throw new BulkConfigurationException(
            "Connector must support at least one of indexed or mapped mappings");
      }

      if (config.hasPath(MAPPING)) {

        if (workflowType == COUNT) {
          throw new BulkConfigurationException(
              "Setting schema.mapping must not be defined when counting rows in a table");
        }

        mapping =
            new MappingInspector(
                config.getString(MAPPING),
                workflowType,
                mappingPreference,
                usingTimestampVariable,
                usingTTLVariable);

        Set<MappingField> fields = mapping.getExplicitVariables().keySet();
        Collection<CQLFragment> variables = mapping.getExplicitVariables().values();

        if (workflowType == LOAD && containsFunctionCalls(variables)) {
          // f1 = now() never allowed when loading
          throw new BulkConfigurationException(
              "Misplaced function call detected on the right side of a mapping entry; "
                  + "please review your schema.mapping setting");
        }
        if (workflowType == UNLOAD && containsFunctionCalls(fields)) {
          // now() = c1 never allowed when unloading
          throw new BulkConfigurationException(
              "Misplaced function call detected on the left side of a mapping entry; "
                  + "please review your schema.mapping setting");
        }

        if (query != null) {
          if (workflowType == LOAD && containsFunctionCalls(fields)) {
            // now() = c1 only allowed if schema.query not present
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when loading if schema.mapping "
                    + "contains a function on the left side of a mapping entry");
          }
          if (workflowType == UNLOAD && containsFunctionCalls(variables)) {
            // f1 = now() only allowed if schema.query not present
            throw new BulkConfigurationException(
                "Setting schema.query must not be defined when unloading if schema.mapping "
                    + "contains a function on the right side of a mapping entry");
          }
        } else {
          writeTimeVariables = mapping.getWriteTimeVariables();
        }
      } else {

        mapping =
            new MappingInspector(
                "*=*", workflowType, mappingPreference, usingTimestampVariable, usingTTLVariable);
      }

      // Misc

      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      allowExtraFields = config.getBoolean(ALLOW_EXTRA_FIELDS);
      allowMissingFields = config.getBoolean(ALLOW_MISSING_FIELDS);
      splits = config.getThreads(SPLITS);

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
    ProtocolVersion protocolVersion =
        session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    if (protocolVersion.compareTo(ProtocolVersion.V4) < 0 && nullToUnset) {
      LOGGER.warn(
          String.format(
              "Protocol version in use (%s) does not support unset bound variables; "
                  + "forcing schema.nullToUnset to false",
              protocolVersion));
      nullToUnset = false;
    }
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

  public List<? extends Statement> createReadStatements(Cluster cluster) {
    ColumnDefinitions variables = preparedStatement.getVariables();
    if (variables.size() == 0) {
      return Collections.singletonList(preparedStatement.bind());
    }
    boolean ok = true;
    Optional<CQLIdentifier> start = queryInspector.getTokenRangeRestrictionStartVariable();
    Optional<CQLIdentifier> end = queryInspector.getTokenRangeRestrictionEndVariable();
    if (!start.isPresent() || !end.isPresent()) {
      ok = false;
    }
    if (start.isPresent() && end.isPresent()) {
      Optional<CQLIdentifier> unrecognized =
          StreamSupport.stream(variables.spliterator(), false)
              .map(Definition::getName)
              .map(CQLIdentifier::fromInternal)
              .filter(name -> !name.equals(start.get()) && !name.equals(end.get()))
              .findAny();
      ok = !unrecognized.isPresent();
    }
    if (!ok) {
      throw new BulkConfigurationException(
          "The provided statement (schema.query) contains unrecognized WHERE restrictions; "
              + "the WHERE clause is only allowed to contain one token range restriction "
              + "of the form: WHERE token(...) > ? AND token(...) <= ?");
    }
    Metadata metadata = cluster.getMetadata();
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    List<TokenRangeReadStatement> statements =
        generator.generate(
            splits,
            range ->
                preparedStatement
                    .bind()
                    .setToken(
                        queryInspector.getTokenRangeRestrictionStartVariableIndex(),
                        metadata.newToken(range.start().toString()))
                    .setToken(
                        queryInspector.getTokenRangeRestrictionEndVariableIndex(),
                        metadata.newToken(range.end().toString())));
    LOGGER.debug("Generated {} bound statements", statements.size());
    return statements;
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

  @NotNull
  private DefaultMapping prepareStatementAndCreateMapping(
      Session session,
      ExtendedCodecRegistry codecRegistry,
      WorkflowType workflowType,
      EnumSet<StatsSettings.StatisticsMode> modes) {
    ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables = null;
    if (!config.hasPath(QUERY)) {
      // in the absence of user-provided queries, create the mapping *before* query generation and
      // preparation
      fieldsToVariables =
          createFieldsToVariablesMap(
              table
                  .getColumns()
                  .stream()
                  .filter(
                      col ->
                          !(col.getName().equals("solr_query")
                              && (col.getType().getName() == Name.VARCHAR
                                  || col.getType().getName() == Name.TEXT)))
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
      fieldsToVariables = processMappingFunctions(fieldsToVariables);
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
      queryInspector = new QueryInspector(query);
    }
    preparedStatement = session.prepare(query);
    if (config.hasPath(QUERY)) {
      // in the presence of user-provided queries, create the mapping *after* query preparation
      ProtocolVersion protocolVersion =
          session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
      if (protocolVersion == V1 && workflowType != LOAD) {
        // In protocol V1 we don't have the result set variables, so we need
        // to work with the information we have from the query inspector, which may be incomplete.
        if (queryInspector.isSelectStar()) {
          fieldsToVariables =
              createFieldsToVariablesMap(
                  table
                      .getColumns()
                      .stream()
                      .map(ColumnMetadata::getName)
                      .map(CQLIdentifier::fromInternal)
                      .collect(Collectors.toList()));
        } else {
          if (queryInspector.hasUnsupportedSelectors()) {
            throw new BulkConfigurationException(
                "Invalid schema.query: the SELECT clause has "
                    + "unsupported selectors; with protocol version 1, "
                    + "only the following selectors are supported: "
                    + "regular column selections, function calls and 'SELECT *'.");
          }
          fieldsToVariables =
              createFieldsToVariablesMap(queryInspector.getResultSetVariables().values());
        }
      } else {
        ColumnDefinitions variables = getVariables(workflowType);
        fieldsToVariables =
            createFieldsToVariablesMap(
                StreamSupport.stream(variables.spliterator(), false)
                    .map(Definition::getName)
                    .map(CQLIdentifier::fromInternal)
                    .collect(Collectors.toList()));
      }
      // validate user-provided query
      if (workflowType == LOAD) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
    }
    assert fieldsToVariables != null;
    return new DefaultMapping(
        processFieldsToVariables(fieldsToVariables),
        codecRegistry,
        processWriteTimeVariables(writeTimeVariables));
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

  private ImmutableMultimap<MappingField, CQLFragment> createFieldsToVariablesMap(
      Collection<CQLFragment> columns) throws BulkConfigurationException {
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
  private AbstractTableMetadata locateTable(
      KeyspaceMetadata keyspace, String tableName, WorkflowType workflowType) {
    AbstractTableMetadata table = keyspace.getTable(quoteIfNecessary(tableName));
    if (table == null) {
      if (workflowType == COUNT || workflowType == UNLOAD) {
        table = keyspace.getMaterializedView(quoteIfNecessary(tableName));
        if (table == null) {
          Optional<MaterializedViewMetadata> match =
              keyspace
                  .getMaterializedViews()
                  .stream()
                  .filter(t -> t.getName().equalsIgnoreCase(tableName))
                  .findFirst();
          if (match.isPresent()) {
            String similarName = quoteIfNecessary(match.get().getName());
            throw new BulkConfigurationException(
                String.format(
                    "Table or materialized view %s does not exist, "
                        + "however a materialized view %s was found. Did you mean to use -t %s?",
                    quoteIfNecessary(tableName), similarName, similarName));
          } else {
            throw new BulkConfigurationException(
                String.format(
                    "Table or materialized view %s does not exist", quoteIfNecessary(tableName)));
          }
        }
      } else {
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
    }
    return table;
  }

  private void validateAllFieldsPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables,
      Collection<CQLFragment> columns) {
    fieldsToVariables.forEach(
        (key, value) -> {
          if (value instanceof CQLIdentifier
              && !isPseudoColumn(value)
              && !columns.contains(value)) {
            if (!config.hasPath(QUERY)) {
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping entry %s doesn't match any column found in table %s",
                      value.render(VARIABLE), tableName));
            } else {
              assert query != null;
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping entry %s doesn't match any bound variable found in query: '%s'",
                      value.render(VARIABLE), query));
            }
          }
        });
  }

  private void validatePrimaryKeyPresent(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    List<ColumnMetadata> partitionKey = table.getPrimaryKey();
    Collection<CQLFragment> mappingVariables = fieldsToVariables.values();
    Map<CQLIdentifier, CQLFragment> queryVariables = queryInspector.getAssignments();
    for (ColumnMetadata pk : partitionKey) {
      CQLIdentifier pkVariable = CQLIdentifier.fromInternal(pk.getName());
      CQLFragment queryVariable = queryVariables.get(pkVariable);
      // the provided query did not contain such column
      if (queryVariable == null) {
        throw new BulkConfigurationException(
            "Missing required primary key column "
                + pkVariable.render(VARIABLE)
                + " from schema.mapping or schema.query");
      }
      // do not check if the mapping contains a PK
      // if the PK is mapped to a function in the query (DAT-326)
      if (!(queryVariable instanceof FunctionCall)) {
        // the mapping did not contain such column
        if (!mappingVariables.contains(queryVariable)) {
          throw new BulkConfigurationException(
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
    sb.append(keyspaceName).append('.').append(tableName).append(" (");
    appendColumnNames(fieldsToVariables, sb, VARIABLE);
    sb.append(") VALUES (");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    CQLRenderMode assignment = protocolVersion != V1 ? NAMED_ASSIGNMENT : POSITIONAL_ASSIGNMENT;
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
        sb.append(((FunctionCall) field).render(assignment));
      } else {
        sb.append(col.render(assignment));
      }
    }
    sb.append(')');
    addTimestampAndTTL(sb);
    return sb.toString();
  }

  private String inferUpdateCounterQuery(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(keyspaceName).append('.').append(tableName);
    // Note: TTL and timestamp are not allowed in counter queries;
    // a test is made inside the following method
    addTimestampAndTTL(sb);
    sb.append(" SET ");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    boolean isFirst = true;
    CQLRenderMode assignment = protocolVersion != V1 ? NAMED_ASSIGNMENT : POSITIONAL_ASSIGNMENT;
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
      // for update queries there can be only one field mapped to a given column
      MappingField field = fieldsToVariables.inverse().get(col).iterator().next();
      if (field instanceof FunctionCall) {
        throw new BulkConfigurationException(
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
          .append(col.render(assignment));
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
      sb.append(col.render(VARIABLE)).append(" = ").append(col.render(assignment));
    }
    return sb.toString();
  }

  private void addTimestampAndTTL(StringBuilder sb) {
    boolean hasTtl = ttlSeconds != -1 || (mapping != null && mapping.hasUsingTTL());
    boolean hasTimestamp =
        timestampMicros != -1 || (mapping != null && mapping.hasUsingTimestamp());
    if (hasTtl || hasTimestamp) {
      if (isCounterTable()) {
        throw new BulkConfigurationException(
            "Cannot set TTL or timestamp when updating a counter table.");
      }
      sb.append(" USING ");
      CQLRenderMode assignment = protocolVersion != V1 ? NAMED_ASSIGNMENT : POSITIONAL_ASSIGNMENT;
      if (hasTtl) {
        sb.append("TTL ");
        if (ttlSeconds != -1) {
          sb.append(ttlSeconds);
        } else {
          sb.append(INTERNAL_TTL_VARNAME.render(assignment));
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
          sb.append(INTERNAL_TIMESTAMP_VARNAME.render(assignment));
        }
      }
    }
  }

  private String inferReadQuery(ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("SELECT ");
    CQLRenderMode mode = protocolVersion != V1 ? ALIASED_SELECTOR : UNALIASED_SELECTOR;
    appendColumnNames(fieldsToVariables, sb, mode);
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName);
    appendTokenRangeRestriction(sb);
    return sb.toString();
  }

  private void appendTokenRangeRestriction(StringBuilder sb) {
    sb.append(" WHERE ");
    appendTokenFunction(sb);
    sb.append(" > ");
    if (protocolVersion != V1) {
      sb.append(":start");
    } else {
      sb.append('?');
    }
    sb.append(" AND ");
    appendTokenFunction(sb);
    sb.append(" <= ");
    if (protocolVersion != V1) {
      sb.append(":end");
    } else {
      sb.append('?');
    }
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
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName);
    appendTokenRangeRestriction(sb);
    return sb.toString();
  }

  @NotNull
  private String getGlobalCountSelector() {
    // When counting global rows we can select anything; we use the first partition key column.
    return quoteIfNecessary(table.getPartitionKey().get(0).getName());
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
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    sb.append("token(");
    Iterator<ColumnMetadata> pks = partitionKey.iterator();
    while (pks.hasNext()) {
      ColumnMetadata pk = pks.next();
      sb.append(quoteIfNecessary(pk.getName()));
      if (pks.hasNext()) {
        sb.append(", ");
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
        sb.append(", ");
      }
    }
  }

  @NotNull
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

  @NotNull
  private static ImmutableMultimap<MappingField, CQLFragment> processMappingFunctions(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    ImmutableMultimap.Builder<MappingField, CQLFragment> builder = ImmutableMultimap.builder();
    for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      if (entry.getKey() instanceof FunctionCall) {
        // functions as fields should be included in the final mapping arg by arg, for every arg
        // that is a field identifier, e.g. plus(fieldA,fieldB) will generate two bound variables:
        // INSERT INTO ... VALUES ( plus(:fieldA,:fieldB) )
        for (CQLFragment arg : ((FunctionCall) entry.getKey()).getArgs()) {
          if (arg instanceof CQLIdentifier) {
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

  @NotNull
  private static ImmutableMultimap<Field, CQLIdentifier> processFieldsToVariables(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    Builder<Field, CQLIdentifier> builder = ImmutableMultimap.builder();
    for (Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      // transform all CQL fragments into CQL identifiers since that's the way they will be searched
      // for in DefaultMapping
      CQLFragment value = entry.getValue();
      String internal = value.render(INTERNAL);
      builder.put(entry.getKey(), CQLIdentifier.fromInternal(internal));
    }
    return builder.build();
  }

  @NotNull
  private static ImmutableSet<CQLIdentifier> processWriteTimeVariables(
      ImmutableSet<CQLFragment> writeTimeVariables) {
    ImmutableSet.Builder<CQLIdentifier> builder = ImmutableSet.builder();
    for (CQLFragment variable : writeTimeVariables) {
      // transform all CQL fragments into CQL identifiers since that's the way they will be searched
      // for in DefaultMapping
      String internal = variable.render(INTERNAL);
      builder.add(CQLIdentifier.fromInternal(internal));
    }
    return builder.build();
  }

  private static boolean containsFunctionCalls(Collection<?> coll) {
    return coll.stream().anyMatch(FunctionCall.class::isInstance);
  }
}
