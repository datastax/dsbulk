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
import static com.datastax.dsbulk.engine.WorkflowType.COUNT;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.MappingInspector;
import com.datastax.dsbulk.engine.internal.schema.QueryInspector;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.utils.StringUtils;
import com.datastax.dsbulk.executor.api.statement.TableScanner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.typesafe.config.ConfigException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

public class SchemaSettings {

  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String KEYSPACE = "keyspace";
  private static final String TABLE = "table";
  private static final String MAPPING = "mapping";
  private static final String ALLOW_EXTRA_FIELDS = "allowExtraFields";
  private static final String ALLOW_MISSING_FIELDS = "allowMissingFields";
  private static final String QUERY = "query";
  private static final String QUERY_TTL = "queryTtl";
  private static final String QUERY_TIMESTAMP = "queryTimestamp";

  private final LoaderConfig config;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private MappingInspector mapping;
  private BiMap<String, String> explicitVariables;
  private String tableName;
  private String keyspaceName;
  private int ttlSeconds;
  private long timestampMicros;
  private TableMetadata table;
  private KeyspaceMetadata keyspace;
  private String query;
  private QueryInspector queryInspector;
  private PreparedStatement preparedStatement;
  private String writeTimeVariable;
  private boolean preferIndexedMapping;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init(WorkflowType workflowType, boolean expectIndexedMapping) {
    try {
      this.preferIndexedMapping = expectIndexedMapping;
      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      ttlSeconds = config.getInt(QUERY_TTL);
      allowExtraFields = config.getBoolean(ALLOW_EXTRA_FIELDS);
      allowMissingFields = config.getBoolean(ALLOW_MISSING_FIELDS);
      String timestampStr = config.getString(QUERY_TIMESTAMP);
      if (timestampStr.isEmpty()) {
        this.timestampMicros = -1L;
      } else {
        try {
          Instant instant = ZonedDateTime.parse(timestampStr).toInstant();
          this.timestampMicros = instantToNumber(instant, MICROSECONDS, EPOCH);
        } catch (Exception e) {
          throw new BulkConfigurationException(
              String.format(
                  "Expecting %s to be in ISO_ZONED_DATE_TIME format but got '%s'",
                  prettyPath(QUERY_TIMESTAMP), timestampStr));
        }
      }
      this.query = config.hasPath(QUERY) ? config.getString(QUERY) : null;

      boolean keyspaceTablePresent = false;
      if (config.hasPath(KEYSPACE)) {
        keyspaceName = Metadata.quoteIfNecessary(config.getString(KEYSPACE));
      }
      if (keyspaceName != null && config.hasPath(TABLE)) {
        keyspaceTablePresent = true;
        tableName = Metadata.quoteIfNecessary(config.getString(TABLE));
      }

      // If table is present, keyspace must be, but not necessarily the other way around.
      if (config.hasPath(TABLE) && keyspaceName == null) {
        throw new BulkConfigurationException(
            prettyPath(KEYSPACE) + " must accompany schema.table in the configuration");
      }

      // If mapping is present, make sure it is parseable as a map.
      if (config.hasPath(MAPPING)) {
        mapping = new MappingInspector(config.getString(MAPPING), preferIndexedMapping);
        if (mapping.isInferring() && !(keyspaceTablePresent || query != null)) {
          throw new BulkConfigurationException(
              String.format(
                  "%s, or %s and %s must be defined when using inferred mapping",
                  prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)));
        }
      } else {
        mapping = null;
      }

      // Either the keyspace and table must be present, or the mapping, or the query must be
      // present.
      if (!config.hasPath(MAPPING) && !config.hasPath(QUERY) && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            String.format(
                "%s, %s, or %s and %s must be defined",
                prettyPath(MAPPING), prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)));
      }

      // Either the keyspace and table must be present, or the mapping must be present.
      if (query == null && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            String.format(
                "%s, or %s and %s must be defined",
                prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)));
      }

      // If a query is provided, ttl and timestamp must not be.
      if (query != null && (timestampMicros != -1 || ttlSeconds != -1)) {
        throw new BulkConfigurationException(
            String.format(
                "%s must not be defined if %s or %s is defined",
                prettyPath(QUERY), prettyPath(QUERY_TTL), prettyPath(QUERY_TIMESTAMP)));
      }

      if (query != null && keyspaceTablePresent) {
        throw new BulkConfigurationException(
            String.format(
                "%s must not be defined if %s and %s are defined",
                prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)));
      }

      if (mapping != null) {
        explicitVariables = mapping.getExplicitVariables();
        // Error out if the explicit variables map timestamp or ttl and
        // there is an explicit query.
        if (query != null) {
          if (explicitVariables.containsValue(INTERNAL_TIMESTAMP_VARNAME)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a field to query-timestamp",
                    prettyPath(QUERY)));
          }
          if (explicitVariables.containsValue(INTERNAL_TTL_VARNAME)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a field to query-ttl", prettyPath(QUERY)));
          }
          if (explicitVariables.keySet().stream().anyMatch(SchemaSettings::isFunction)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a function to a column",
                    prettyPath(QUERY)));
          }
        }
        // store the write time variable name for later if it was present in the mapping
        if (explicitVariables.containsValue(INTERNAL_TIMESTAMP_VARNAME)) {
          writeTimeVariable = INTERNAL_TIMESTAMP_VARNAME;
        }
      } else {
        explicitVariables = null;
      }

      if (query != null) {
        queryInspector = new QueryInspector(query);
        // If a query is provided, check now if it contains a USING TIMESTAMP variable,
        // and get its name.
        writeTimeVariable = queryInspector.getWriteTimeVariable();
      }

      if (workflowType == COUNT) {
        if (config.hasPath(MAPPING)) {
          throw new BulkConfigurationException(
              String.format(
                  "%s must not be defined when counting rows in a table", prettyPath(MAPPING)));
        }
      }

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "schema");
    } catch (IllegalArgumentException e) {
      throw new BulkConfigurationException(e);
    }
  }

  public RecordMapper createRecordMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    DefaultMapping mapping = prepareStatementAndCreateMapping(session, codecRegistry, LOAD);
    return new DefaultRecordMapper(
        preparedStatement,
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
    DefaultMapping mapping = prepareStatementAndCreateMapping(session, codecRegistry, UNLOAD);
    return new DefaultReadResultMapper(mapping, recordMetadata);
  }

  public ReadResultCounter createReadResultCounter(
      Session session,
      ExtendedCodecRegistry codecRegistry,
      EnumSet<StatsSettings.StatisticsMode> modes,
      int numPartitions) {
    prepareStatementAndCreateMapping(session, null, COUNT);
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

  public List<Statement> createReadStatements(Cluster cluster) {
    ColumnDefinitions variables = preparedStatement.getVariables();
    if (variables.size() == 0) {
      return Collections.singletonList(preparedStatement.bind());
    }
    List<String> unrecognized =
        StreamSupport.stream(variables.spliterator(), false)
            .map(ColumnDefinitions.Definition::getName)
            .filter(name -> !name.equals("start") && !name.equals("end"))
            .collect(Collectors.toList());
    if (!unrecognized.isEmpty()) {
      throw new BulkConfigurationException(
          String.format(
              "The provided statement (schema.query) contains unrecognized bound variables: %s; "
                  + "only 'start' and 'end' can be used to define a token range",
              unrecognized));
    }
    Set<TokenRange> ring = cluster.getMetadata().getTokenRanges();
    return TableScanner.scan(
        ring,
        range ->
            preparedStatement
                .bind()
                .setToken("start", range.getStart())
                .setToken("end", range.getEnd()));
  }

  @NotNull
  private DefaultMapping prepareStatementAndCreateMapping(
      Session session, ExtendedCodecRegistry codecRegistry, WorkflowType workflowType) {
    BiMap<String, String> fieldsToVariables = null;
    if (!config.hasPath(QUERY)) {
      // in the absence of user-provided queries, create the mapping *before* query generation and
      // preparation
      inferKeyspaceAndTable(session);
      fieldsToVariables =
          createFieldsToVariablesMap(
              () ->
                  table
                      .getColumns()
                      .stream()
                      .map(ColumnMetadata::getName)
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
        query = inferCountQuery();
      }
      queryInspector = new QueryInspector(query);
      // validate generated query
      if (workflowType == LOAD) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
      // remove function mappings as we won't need them anymore from now on
      fieldsToVariables = removeMappingFunctions(fieldsToVariables);
    }
    if (keyspaceName != null) {
      // keyspace is already properly quoted
      session.execute("USE " + keyspaceName);
    }
    preparedStatement = session.prepare(query);
    if (config.hasPath(QUERY)) {
      // in the presence of user-provided queries, create the mapping *after* query preparation
      ColumnDefinitions variables = getVariables(workflowType);
      fieldsToVariables =
          createFieldsToVariablesMap(
              () ->
                  StreamSupport.stream(variables.spliterator(), false)
                      .map(ColumnDefinitions.Definition::getName)
                      .collect(Collectors.toList()));
      inferKeyspaceAndTableCustom(session);
      // validate user-provided query
      if (workflowType == LOAD) {
        validatePrimaryKeyPresent(fieldsToVariables);
      } else if (workflowType == COUNT) {
        validatePartitionKeyPresent();
      }
    }
    assert fieldsToVariables != null;
    assert keyspace != null;
    assert table != null;
    assert keyspaceName != null;
    assert tableName != null;
    assert query != null;
    return new DefaultMapping(
        ImmutableBiMap.copyOf(fieldsToVariables), codecRegistry, writeTimeVariable);
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

  private BiMap<String, String> createFieldsToVariablesMap(Supplier<List<String>> columns)
      throws BulkConfigurationException {
    BiMap<String, String> fieldsToVariables;
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

      if (!explicitVariables.isEmpty()) {
        ImmutableBiMap.Builder<String, String> builder = ImmutableBiMap.builder();
        for (Map.Entry<String, String> entry : explicitVariables.entrySet()) {
          builder.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : fieldsToVariables.entrySet()) {
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
    validateMappedRecords(fieldsToVariables);

    return fieldsToVariables;
  }

  /** Version used with generated queries. */
  private void inferKeyspaceAndTable(Session session) {
    assert !config.hasPath(QUERY);
    assert keyspaceName != null && tableName != null;
    Metadata metadata = session.getCluster().getMetadata();
    keyspace = metadata.getKeyspace(keyspaceName);
    if (keyspace == null) {
      String lowerCaseKeyspaceName = ParseUtils.unDoubleQuote(keyspaceName).toLowerCase();
      if (metadata.getKeyspace(lowerCaseKeyspaceName) != null) {
        throw new IllegalArgumentException(
            String.format(
                "Keyspace %s does not exist, however a keyspace %s was found. Did you mean to use -k %s?",
                keyspaceName, lowerCaseKeyspaceName, lowerCaseKeyspaceName));
      } else {
        throw new IllegalArgumentException(
            String.format("Keyspace %s does not exist", keyspaceName));
      }
    }
    table = keyspace.getTable(tableName);
    if (table == null) {
      String lowerCaseTableName = ParseUtils.unDoubleQuote(tableName).toLowerCase();
      if (keyspace.getTable(lowerCaseTableName) != null) {
        throw new IllegalArgumentException(
            String.format(
                "Table %s does not exist, however a table %s was found. Did you mean to use -t %s?",
                tableName, lowerCaseTableName, lowerCaseTableName));
      } else {
        throw new IllegalArgumentException(String.format("Table %s does not exist", tableName));
      }
    }
  }

  /** Version used with user-supplied queries. */
  private void inferKeyspaceAndTableCustom(Session session) {
    assert config.hasPath(QUERY);
    if (keyspace == null) {
      if (keyspaceName == null) {
        keyspaceName = Metadata.quoteIfNecessary(queryInspector.getKeyspaceName());
      }
      keyspace = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      if (keyspace == null) {
        throw new BulkConfigurationException(
            "Could not infer the target keyspace form the provided statement (schema.query).");
      }
    }
    if (table == null) {
      if (tableName == null) {
        tableName = Metadata.quoteIfNecessary(queryInspector.getTableName());
      }
      table = keyspace.getTable(tableName);
      if (table == null) {
        throw new BulkConfigurationException(
            "Could not infer the target table form the provided statement (schema.query).");
      }
    }
  }

  private void validateMappedRecords(BiMap<String, String> fieldsToVariables) {
    if (preferIndexedMapping && !isIndexed(fieldsToVariables.keySet())) {
      throw new BulkConfigurationException(
          "Schema mapping contains named fields, but connector only supports indexed fields, "
              + "please enable support for named fields in the connector, or alternatively, "
              + "provide an indexed mapping of the form: '0=col1,1=col2,...'");
    }
  }

  private void validateAllFieldsPresent(
      BiMap<String, String> fieldsToVariables, Supplier<List<String>> columns) {
    List<String> colNames = columns.get();
    fieldsToVariables.forEach(
        (key, value) -> {
          if (!isPseudoColumn(value) && !colNames.contains(value)) {
            if (table != null) {
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping %s doesn't match any column found in table %s",
                      value, table.getName()));
            } else {
              assert query != null;
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping %s doesn't match any bound variable found in query: '%s'",
                      value, query));
            }
          }
        });
  }

  private void validatePrimaryKeyPresent(BiMap<String, String> fieldsToVariables) {
    List<ColumnMetadata> partitionKey = table.getPrimaryKey();
    for (ColumnMetadata pk : partitionKey) {
      String variable = queryInspector.getColumnsToVariables().get(pk.getName());
      if (!fieldsToVariables.containsValue(variable)) {
        throw new BulkConfigurationException(
            "Missing required primary key column "
                + Metadata.quoteIfNecessary(pk.getName())
                + " from schema.mapping or schema.query");
      }
    }
  }

  // Used for the count workflow only.
  private void validatePartitionKeyPresent() {
    // the query must contain the entire partition key in the select clause,
    // and nothing else.
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    Set<String> columns = new HashSet<>(queryInspector.getColumnsToVariables().values());
    for (ColumnMetadata pk : partitionKey) {
      if (!columns.remove(pk.getName())) {
        throw new BulkConfigurationException(
            "Missing required partition key column "
                + Metadata.quoteIfNecessary(pk.getName())
                + " from schema.query");
      }
    }
    if (!columns.isEmpty()) {
      String offendingColumns =
          columns.stream().map(Metadata::quoteIfNecessary).collect(Collectors.joining(", "));
      throw new BulkConfigurationException(
          String.format(
              "The provided statement (schema.query) contains extraneous columns in the SELECT clause: "
                  + "%s; it should contain only partition key columns.",
              offendingColumns));
    }
  }

  private ImmutableBiMap<String, String> inferFieldsToVariablesMap(Supplier<List<String>> columns) {

    // use a builder to preserve iteration order
    ImmutableBiMap.Builder<String, String> fieldsToVariables = new ImmutableBiMap.Builder<>();

    int i = 0;
    for (String colName : columns.get()) {
      if (mapping == null || !mapping.getExcludedVariables().contains(colName)) {
        // don't quote column names here, it will be done later on if required
        // for unload only, use the query's variable order
        if (preferIndexedMapping) {
          fieldsToVariables.put(Integer.toString(i), colName);
        } else {
          fieldsToVariables.put(colName, colName);
        }
      }
      i++;
    }
    return fieldsToVariables.build();
  }

  private String inferInsertQuery(BiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    appendColumnNames(fieldsToVariables, sb);
    sb.append(") VALUES (");
    Set<String> cols = maybeSortCols(fieldsToVariables);
    Iterator<String> it = cols.iterator();
    boolean isFirst = true;
    while (it.hasNext()) {
      String col = it.next();
      if (isPseudoColumn(col)) {
        // This isn't a real column name.
        continue;
      }

      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      String field = fieldsToVariables.inverse().get(col);
      if (isFunction(field)) {
        // Assume this is a function call that should be placed directly in the query.
        sb.append(field);
      } else {
        sb.append(':');
        sb.append(Metadata.quoteIfNecessary(col));
      }
    }
    sb.append(')');
    addTimestampAndTTL(fieldsToVariables, sb);
    return sb.toString();
  }

  private String inferUpdateCounterQuery(BiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(keyspaceName).append('.').append(tableName);
    // Note: TTL and timestamp are not allowed in counter queries;
    // a test is made inside the following method
    addTimestampAndTTL(fieldsToVariables, sb);
    sb.append(" SET ");
    Set<String> cols = maybeSortCols(fieldsToVariables);
    Iterator<String> it = cols.iterator();
    boolean isFirst = true;
    List<String> pks =
        table.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
    while (it.hasNext()) {
      String col = it.next();
      if (pks.contains(col)) {
        continue;
      }
      if (isFunction(fieldsToVariables.inverse().get(col))) {
        throw new BulkConfigurationException(
            "Function calls are not allowed when updating a counter table.");
      }
      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      String quoted = Metadata.quoteIfNecessary(col);
      sb.append(quoted).append('=').append(quoted).append("+:").append(quoted);
    }
    sb.append(" WHERE ");
    it = pks.iterator();
    isFirst = true;
    while (it.hasNext()) {
      String col = it.next();
      if (!isFirst) {
        sb.append(" AND ");
      }
      isFirst = false;
      sb.append(Metadata.quoteIfNecessary(col)).append("=:").append(Metadata.quoteIfNecessary(col));
    }
    return sb.toString();
  }

  private void addTimestampAndTTL(BiMap<String, String> fieldsToVariables, StringBuilder sb) {
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

  private String inferReadQuery(BiMap<String, String> fieldsToVariables) {
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    StringBuilder sb = new StringBuilder("SELECT ");
    appendColumnNames(fieldsToVariables, sb);
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName).append(" WHERE ");
    appendTokenFunction(sb, partitionKey);
    sb.append(" > :start AND ");
    appendTokenFunction(sb, partitionKey);
    sb.append(" <= :end");
    return sb.toString();
  }

  private String inferCountQuery() {
    StringBuilder sb = new StringBuilder("SELECT ");
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    Iterator<ColumnMetadata> it = partitionKey.iterator();
    while (it.hasNext()) {
      ColumnMetadata col = it.next();
      sb.append(Metadata.quoteIfNecessary(col.getName()));
      if (it.hasNext()) {
        sb.append(',');
      }
    }
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName).append(" WHERE ");
    appendTokenFunction(sb, partitionKey);
    sb.append(" > :start AND ");
    appendTokenFunction(sb, partitionKey);
    sb.append(" <= :end");
    return sb.toString();
  }

  private static void appendColumnNames(BiMap<String, String> fieldsToVariables, StringBuilder sb) {
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<String> cols = maybeSortCols(fieldsToVariables);
    Iterator<String> it = cols.iterator();
    boolean isFirst = true;
    while (it.hasNext()) {
      // this assumes that the variable name found in the mapping
      // corresponds to a CQL column having the exact same name.
      String col = it.next();
      if (isPseudoColumn(col)) {
        // This is not a real column. Skip it.
        continue;
      }

      if (!isFirst) {
        sb.append(',');
      }
      isFirst = false;
      sb.append(Metadata.quoteIfNecessary(col));
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

  @NotNull
  private static Set<String> maybeSortCols(BiMap<String, String> fieldsToVariables) {
    Set<String> cols;
    if (isIndexed(fieldsToVariables.keySet())) {
      // order columns by index
      BiMap<String, String> variablesToFields = fieldsToVariables.inverse();
      cols =
          new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(variablesToFields.get(o))));
      cols.addAll(fieldsToVariables.values());
    } else {
      // preserve original order of variables in the mapping
      cols = new LinkedHashSet<>(fieldsToVariables.values());
    }
    return cols;
  }

  private static boolean isIndexed(Set<String> keys) {
    return keys.stream().allMatch(s -> s.matches("\\d+"));
  }

  private static boolean isFunction(String field) {
    // If a field contains a paren, interpret it to be a cql function call.
    return field.contains("(");
  }

  private static boolean isPseudoColumn(String col) {
    return col.equals(INTERNAL_TTL_VARNAME) || col.equals(INTERNAL_TIMESTAMP_VARNAME);
  }

  private static String prettyPath(String path) {
    return String.format("schema%s%s", StringUtils.DELIMITER, path);
  }

  private static BiMap<String, String> removeMappingFunctions(
      BiMap<String, String> fieldsToVariables) {
    ImmutableBiMap.Builder<String, String> builder = ImmutableBiMap.builder();
    for (Map.Entry<String, String> entry : fieldsToVariables.entrySet()) {
      if (!isFunction(entry.getKey())) {
        builder.put(entry);
      }
    }
    return builder.build();
  }
}
