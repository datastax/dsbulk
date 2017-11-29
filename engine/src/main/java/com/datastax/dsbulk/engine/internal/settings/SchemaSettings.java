/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DriverCoreHooks.resultSetVariables;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToTimestampSinceEpoch;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.MergedRecordMetadata;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.utils.StringUtils;
import com.datastax.dsbulk.executor.api.statement.TableScanner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

public class SchemaSettings {

  private static final String TTL_VARNAME = "dsbulk_internal_ttl";
  private static final String TIMESTAMP_VARNAME = "dsbulk_internal_timestamp";

  private static final String INFERRED_MAPPING_TOKEN = "__INFERRED_MAPPING";
  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String NULL_STRINGS = "nullStrings";
  private static final String KEYSPACE = "keyspace";
  private static final String TABLE = "table";
  private static final String MAPPING = "mapping";
  private static final String QUERY = "query";
  private static final String RECORD_METADATA = "recordMetadata";
  private static final String QUERY_TTL = "queryTtl";
  private static final String QUERY_TIMESTAMP = "queryTimestamp";

  // A mapping spec may refer to these special variables which are used to bind
  // input fields to the write timestamp or ttl of the record.

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  private static final Pattern WRITETIME_PATTERN =
      Pattern.compile(
          "(?:USING|AND)\\s+TIMESTAMP\\s+:([a-zA-Z][a-zA-Z0-9_]*|\".+(?:(?<![\"])[\"]))",
          Pattern.CASE_INSENSITIVE);

  private final LoaderConfig config;

  private ImmutableSet<String> nullStrings;
  private boolean nullToUnset;
  private Config mapping;
  private BiMap<String, String> explicitVariables;
  private String tableName;
  private String keyspaceName;
  private int ttlSeconds;
  private long timestampMicros;
  private TableMetadata table;
  private String query;
  private PreparedStatement preparedStatement;
  private String writeTimeVariable;

  SchemaSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init(StringToInstantCodec timestampCodec) {
    try {
      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      nullStrings = ImmutableSet.copyOf(config.getStringList(NULL_STRINGS));
      ttlSeconds = config.getInt(QUERY_TTL);
      String timestampStr = config.getString(QUERY_TIMESTAMP);
      if (timestampStr.isEmpty()) {
        this.timestampMicros = -1L;
      } else {
        try {
          Instant instant = timestampCodec.convertFrom(timestampStr);
          this.timestampMicros = instantToTimestampSinceEpoch(instant, MICROSECONDS, EPOCH);
        } catch (Exception e) {
          throw new BulkConfigurationException(
              String.format("Could not parse %s '%s'", prettyPath(QUERY_TIMESTAMP), timestampStr),
              prettyPath(QUERY_TIMESTAMP));
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
            prettyPath(KEYSPACE) + " must accompany schema.table in the configuration", "schema");
      }

      // If mapping is present, make sure it is parseable as a map.
      if (config.hasPath(MAPPING)) {
        mapping = getMapping();
        if (mapping.hasPath(INFERRED_MAPPING_TOKEN) && !(keyspaceTablePresent || query != null)) {
          throw new BulkConfigurationException(
              String.format(
                  "%s or %s and %s must be defined when using inferred mapping",
                  prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)),
              "schema");
        }
      } else {
        mapping = null;
      }

      // Either the keyspace and table must be present, or the mapping, or the query must be present.
      if (!config.hasPath(MAPPING) && !config.hasPath(QUERY) && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            String.format(
                "%s, %s, or %s and %s must be defined",
                prettyPath(MAPPING), prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)),
            "schema");
      }

      // Either the keyspace and table must be present, or the mapping must be present.
      if (query == null && !keyspaceTablePresent) {
        throw new BulkConfigurationException(
            String.format(
                "%s, or %s and %s must be defined",
                prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)),
            "schema");
      }

      // If a query is provided, ttl and timestamp must not be.
      if (query != null && (timestampMicros != -1 || ttlSeconds != -1)) {
        throw new BulkConfigurationException(
            String.format(
                "%s must not be defined if %s or %s is defined",
                prettyPath(QUERY), prettyPath(QUERY_TTL), prettyPath(QUERY_TIMESTAMP)),
            "schema");
      }

      if (query != null && (keyspaceName != null || tableName != null)) {
        throw new BulkConfigurationException(
            String.format(
                "%s must not be defined if %s and %s are defined",
                prettyPath(QUERY), prettyPath(KEYSPACE), prettyPath(TABLE)),
            "schema");
      }

      if (mapping != null) {
        explicitVariables = HashBiMap.create();
        for (String fieldName : mapping.withoutPath(INFERRED_MAPPING_TOKEN).root().keySet()) {
          String variableName = mapping.getString(fieldName);

          // Rename the user-specified __ttl and __timestamp vars to the (legal) bound variable
          // names.
          if (variableName.equals(EXTERNAL_TTL_VARNAME)) {
            variableName = TTL_VARNAME;
          } else if (variableName.equals(EXTERNAL_TIMESTAMP_VARNAME)) {
            variableName = TIMESTAMP_VARNAME;
            // store the write time variable name for later
            writeTimeVariable = TIMESTAMP_VARNAME;
          }

          if (explicitVariables.containsValue(variableName)) {
            if (variableName.equals(explicitVariables.get(fieldName))) {
              // This mapping already exists. Skip it.
              continue;
            }
            throw new BulkConfigurationException(
                "Multiple input values in mapping resolve to column "
                    + mapping.getString(fieldName)
                    + ". "
                    + "Please review schema.mapping for duplicates.",
                "schema.mapping");
          }
          explicitVariables.put(fieldName, variableName);
        }

        // Error out if the explicit variables map timestamp or ttl and
        // there is an explicit query.
        if (query != null) {
          if (explicitVariables.containsValue(TIMESTAMP_VARNAME)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a field to query-timestamp",
                    prettyPath(QUERY)),
                "schema");
          }
          if (explicitVariables.containsValue(TTL_VARNAME)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a field to query-ttl", prettyPath(QUERY)),
                "schema");
          }
          if (explicitVariables.keySet().stream().anyMatch(SchemaSettings::isFunction)) {
            throw new BulkConfigurationException(
                String.format(
                    "%s must not be defined when mapping a function to a column",
                    prettyPath(QUERY)),
                "schema");
          }
        }
      } else {
        explicitVariables = null;
      }

      // If a query is provided, check now if it contains a USING TIMESTAMP variable,
      // and get its name
      if (query != null) {
        writeTimeVariable = inferWriteTimeVariable();
      }

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "schema");
    } catch (IllegalArgumentException e) {
      throw new BulkConfigurationException(e, "schema");
    }
  }

  public RecordMapper createRecordMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    DefaultMapping mapping = prepareStatementAndCreateMapping(session, codecRegistry, LOAD);
    return new DefaultRecordMapper(
        preparedStatement, mapping, mergeRecordMetadata(recordMetadata), nullStrings, nullToUnset);
  }

  public ReadResultMapper createReadResultMapper(
      Session session, RecordMetadata recordMetadata, ExtendedCodecRegistry codecRegistry)
      throws BulkConfigurationException {
    DefaultMapping mapping = prepareStatementAndCreateMapping(session, codecRegistry, UNLOAD);
    return new DefaultReadResultMapper(
        mapping,
        mergeRecordMetadata(recordMetadata),
        nullStrings.isEmpty() ? null : nullStrings.iterator().next());
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

  public String getKeyspace() {
    return keyspaceName;
  }

  @NotNull
  private DefaultMapping prepareStatementAndCreateMapping(
      Session session, ExtendedCodecRegistry codecRegistry, WorkflowType workflowType) {
    BiMap<String, String> fieldsToVariables = null;
    if (query == null) {
      fieldsToVariables =
          createFieldsToVariablesMap(
              session,
              () ->
                  table
                      .getColumns()
                      .stream()
                      .map(ColumnMetadata::getName)
                      .collect(Collectors.toList()));
      query =
          workflowType == WorkflowType.LOAD
              ? inferWriteQuery(fieldsToVariables)
              : inferReadQuery(fieldsToVariables);
      fieldsToVariables.keySet().removeIf(SchemaSettings::isFunction);
    }
    preparedStatement = session.prepare(query);
    if (fieldsToVariables == null) {
      fieldsToVariables =
          createFieldsToVariablesMap(
              session,
              () -> {
                switch (workflowType) {
                  case LOAD:
                    return StreamSupport.stream(
                            preparedStatement.getVariables().spliterator(), false)
                        .map(ColumnDefinitions.Definition::getName)
                        .collect(Collectors.toList());
                  case UNLOAD:
                    return StreamSupport.stream(
                            resultSetVariables(preparedStatement).spliterator(), false)
                        .map(ColumnDefinitions.Definition::getName)
                        .collect(Collectors.toList());
                  default:
                    throw new AssertionError();
                }
              });
    }
    return new DefaultMapping(
        ImmutableBiMap.copyOf(fieldsToVariables), codecRegistry, writeTimeVariable);
  }

  @NotNull
  private BiMap<String, String> createFieldsToVariablesMap(
      Session session, Supplier<List<String>> columns) throws BulkConfigurationException {
    BiMap<String, String> fieldsToVariables = null;

    if (keyspaceName != null && tableName != null) {
      KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      Preconditions.checkNotNull(keyspace, "Keyspace does not exist: " + keyspaceName);
      table = keyspace.getTable(tableName);
      Preconditions.checkNotNull(
          table, String.format("Table does not exist: %s.%s", keyspaceName, tableName));
    }

    if (mapping == null) {
      fieldsToVariables = inferFieldsToVariablesMap(columns);
    } else {
      if (mapping.hasPath(INFERRED_MAPPING_TOKEN)) {
        fieldsToVariables =
            inferFieldsToVariablesMap(
                new InferredMappingSpec(mapping.getValue(INFERRED_MAPPING_TOKEN)), columns);
      }
      if (fieldsToVariables == null) {
        fieldsToVariables = HashBiMap.create();
      }

      for (Map.Entry<String, String> entry : explicitVariables.entrySet()) {
        fieldsToVariables.forcePut(entry.getKey(), entry.getValue());
      }
    }

    validateAllFieldsPresent(fieldsToVariables, columns);

    // It's tempting to change this check to simply check the query data member.
    // At the time of this writing, that would be totally safe; however, that
    // member is not final, which leaves the possibility of it being initialized
    // after the constructor but before this method is called (with the inferred query).
    //
    // We really want to know if the *user* provided a query, and only validate
    // if he didn't. So, we go to the source: the config object.
    if (!config.hasPath(QUERY)) {
      validateAllKeysPresent(fieldsToVariables);
    }

    Preconditions.checkNotNull(
        fieldsToVariables,
        "Mapping was absent and could not be inferred, please provide an explicit mapping");

    return fieldsToVariables;
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
                      value, table.getName()),
                  "schema.mapping");
            } else {
              assert query != null;
              throw new BulkConfigurationException(
                  String.format(
                      "Schema mapping %s doesn't match any bound variable found in query: '%s'",
                      value, query),
                  "schema.mapping");
            }
          }
        });
  }

  private void validateAllKeysPresent(BiMap<String, String> fieldsToVariables) {
    assert table != null;
    List<ColumnMetadata> primaryKeys = table.getPrimaryKey();
    primaryKeys.forEach(
        key -> {
          if (!fieldsToVariables.containsValue(key.getName())) {
            throw new BulkConfigurationException(
                "Missing required key column of "
                    + key.getName()
                    + " from header or schema.mapping. Please ensure it's included in the header or mapping",
                "schema.mapping");
          }
        });
  }

  private Config getMapping() throws BulkConfigurationException {
    String mappingString = config.getString(MAPPING).replaceAll("\\*", INFERRED_MAPPING_TOKEN);
    try {
      return ConfigFactory.parseString(mappingString);
    } catch (ConfigException.Parse e) {
      // mappingString doesn't seem to be a map. Treat it as a list instead.
      Map<String, String> indexMap = new HashMap<>();
      int curInd = 0;
      for (String s : config.getStringList(MAPPING)) {
        indexMap.put(Integer.toString(curInd++), s);
      }
      return ConfigFactory.parseMap(indexMap);
    }
  }

  private RecordMetadata mergeRecordMetadata(RecordMetadata fallback) {
    if (config.hasPath(RECORD_METADATA)) {
      ImmutableMap.Builder<String, TypeToken<?>> fieldsToTypes = new ImmutableMap.Builder<>();
      LoaderConfig recordMetadata =
          new DefaultLoaderConfig(ConfigFactory.parseString(config.getString(RECORD_METADATA)));
      for (String path : recordMetadata.root().keySet()) {
        fieldsToTypes.put(path, TypeToken.of(recordMetadata.getClass(path)));
      }
      return new MergedRecordMetadata(fieldsToTypes.build(), fallback);
    }
    return fallback;
  }

  private BiMap<String, String> inferFieldsToVariablesMap(Supplier<List<String>> columns) {
    return inferFieldsToVariablesMap(null, columns);
  }

  private BiMap<String, String> inferFieldsToVariablesMap(
      InferredMappingSpec spec, Supplier<List<String>> columns) {
    HashBiMap<String, String> fieldsToVariables = HashBiMap.create();
    for (String colName : columns.get()) {
      if (spec == null || spec.allow(colName)) {
        // don't quote column names here, it will be done later on if required
        fieldsToVariables.put(colName, colName);
      }
    }
    return fieldsToVariables;
  }

  private String inferWriteQuery(BiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName).append('.').append(tableName).append('(');
    appendColumnNames(fieldsToVariables, sb);
    sb.append(") VALUES (");
    Set<String> cols = new LinkedHashSet<>(fieldsToVariables.values());
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
    boolean hasTtl = ttlSeconds != -1 || fieldsToVariables.containsValue(TTL_VARNAME);
    boolean hasTimestamp =
        timestampMicros != -1 || fieldsToVariables.containsValue(TIMESTAMP_VARNAME);
    if (hasTtl || hasTimestamp) {
      sb.append(" USING ");
      if (hasTtl) {
        sb.append("TTL ");
        if (ttlSeconds != -1) {
          sb.append(ttlSeconds);
        } else {
          sb.append(':');
          sb.append(TTL_VARNAME);
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
          sb.append(TIMESTAMP_VARNAME);
        }
      }
    }
    return sb.toString();
  }

  private String inferReadQuery(BiMap<String, String> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("SELECT ");
    appendColumnNames(fieldsToVariables, sb);
    sb.append(" FROM ").append(keyspaceName).append('.').append(tableName).append(" WHERE ");
    appendTokenFunction(sb, table.getPartitionKey());
    sb.append(" > :start AND ");
    appendTokenFunction(sb, table.getPartitionKey());
    sb.append(" <= :end");
    return sb.toString();
  }

  private String inferWriteTimeVariable() {
    Matcher matcher = WRITETIME_PATTERN.matcher(query);
    if (matcher.find()) {
      return DriverCoreHooks.handleId(matcher.group(1));
    }
    return null;
  }

  private static void appendColumnNames(BiMap<String, String> fieldsToVariables, StringBuilder sb) {
    // de-dup in case the mapping has both indexed and mapped entries
    // for the same bound variable
    Set<String> cols = new LinkedHashSet<>(fieldsToVariables.values());
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

  private static boolean isFunction(String field) {
    // If a field contains a paren, interpret it to be a cql function call.
    return field.contains("(");
  }

  private static boolean isPseudoColumn(String col) {
    return col.equals(TTL_VARNAME) || col.equals(TIMESTAMP_VARNAME);
  }

  private static String prettyPath(String path) {
    return String.format("schema%s%s", StringUtils.DELIMITER, path);
  }

  private class InferredMappingSpec {
    private final Set<String> excludes = new HashSet<>();

    InferredMappingSpec(ConfigValue spec) {
      if (spec.valueType() == ConfigValueType.STRING) {
        processSpec((String) spec.unwrapped());
      } else if (spec.valueType() == ConfigValueType.LIST) {
        @SuppressWarnings("unchecked")
        List<Object> specList = (List<Object>) spec.unwrapped();
        specList.forEach(x -> processSpec((String) x));
      }
    }

    private void processSpec(String specString) {
      if (specString.startsWith("-")) {
        // We're excluding a particular column. This implies that
        // we include all others.
        excludes.add(specString.substring(1));
      }
    }

    boolean allow(String name) {
      return !excludes.contains(name);
    }
  }
}
