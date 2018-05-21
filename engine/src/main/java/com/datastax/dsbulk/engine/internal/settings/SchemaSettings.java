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
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.cql3.CqlBaseListener;
import com.datastax.dsbulk.commons.cql3.CqlLexer;
import com.datastax.dsbulk.commons.cql3.CqlParser;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.schema.DefaultMapping;
import com.datastax.dsbulk.engine.internal.schema.DefaultReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.DefaultRecordMapper;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.utils.StringUtils;
import com.datastax.dsbulk.executor.api.statement.TableScanner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSettings.class);

  private static final String TTL_VARNAME = "dsbulk_internal_ttl";
  private static final String TIMESTAMP_VARNAME = "dsbulk_internal_timestamp";

  private static final String INFERRED_MAPPING_TOKEN = "__INFERRED_MAPPING";
  private static final String NULL_TO_UNSET = "nullToUnset";
  private static final String KEYSPACE = "keyspace";
  private static final String TABLE = "table";
  private static final String MAPPING = "mapping";
  private static final String ALLOW_EXTRA_FIELDS = "allowExtraFields";
  private static final String ALLOW_MISSING_FIELDS = "allowMissingFields";
  private static final String QUERY = "query";
  private static final String QUERY_TTL = "queryTtl";
  private static final String QUERY_TIMESTAMP = "queryTimestamp";

  // A mapping spec may refer to these special variables which are used to bind
  // input fields to the write timestamp or ttl of the record.

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  private final LoaderConfig config;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private String mappingString;
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

  public void init() {
    try {
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
        mapping = getMapping();
        if (mapping.hasPath(INFERRED_MAPPING_TOKEN) && !(keyspaceTablePresent || query != null)) {
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
        Map<String, String> explicitVariables = new LinkedHashMap<>();
        for (String fieldName : orderedMappingKeys()) {
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
                    + "Please review schema.mapping for duplicates.");
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
                    prettyPath(QUERY)));
          }
          if (explicitVariables.containsValue(TTL_VARNAME)) {
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
        // preserve iteration order
        this.explicitVariables = ImmutableBiMap.copyOf(explicitVariables);
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
      throw new BulkConfigurationException(e);
    }
  }

  public RecordMapper createRecordMapper(
      Session session,
      RecordMetadata recordMetadata,
      ExtendedCodecRegistry codecRegistry,
      boolean expectIndexedMapping)
      throws BulkConfigurationException {
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(session, codecRegistry, LOAD, expectIndexedMapping);
    return new DefaultRecordMapper(
        preparedStatement,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields);
  }

  public ReadResultMapper createReadResultMapper(
      Session session,
      RecordMetadata recordMetadata,
      ExtendedCodecRegistry codecRegistry,
      boolean expectIndexedMapping)
      throws BulkConfigurationException {
    // wo don't check that mapping records are supported when unloading, the only thing that matters
    // is the order in which fields appear in the record.
    DefaultMapping mapping =
        prepareStatementAndCreateMapping(session, codecRegistry, UNLOAD, expectIndexedMapping);
    return new DefaultReadResultMapper(mapping, recordMetadata);
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

  public String getKeyspace() {
    return keyspaceName;
  }

  private DefaultMapping prepareStatementAndCreateMapping(
      Session session,
      ExtendedCodecRegistry codecRegistry,
      WorkflowType workflowType,
      boolean expectIndexedMapping) {
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
                      .collect(Collectors.toList()),
              workflowType,
              expectIndexedMapping);
      query =
          workflowType == WorkflowType.LOAD
              ? inferWriteQuery(fieldsToVariables)
              : inferReadQuery(fieldsToVariables);
      // remove function mappings as we won't need them anymore from now on
      fieldsToVariables = removeMappingFunctions(fieldsToVariables);
    }
    preparedStatement = session.prepare(query);
    if (fieldsToVariables == null) {
      fieldsToVariables =
          createFieldsToVariablesMap(
              session,
              () -> {
                ColumnDefinitions variables = getVariables(workflowType);
                return StreamSupport.stream(variables.spliterator(), false)
                    .map(ColumnDefinitions.Definition::getName)
                    .collect(Collectors.toList());
              },
              workflowType,
              expectIndexedMapping);
    }
    return new DefaultMapping(
        ImmutableBiMap.copyOf(fieldsToVariables), codecRegistry, writeTimeVariable);
  }

  private ColumnDefinitions getVariables(WorkflowType workflowType) {
    switch (workflowType) {
      case LOAD:
        return preparedStatement.getVariables();
      case UNLOAD:
        return resultSetVariables(preparedStatement);
      default:
        throw new AssertionError();
    }
  }

  private BiMap<String, String> createFieldsToVariablesMap(
      Session session,
      Supplier<List<String>> columns,
      WorkflowType workflowType,
      boolean expectIndexedMapping)
      throws BulkConfigurationException {
    BiMap<String, String> fieldsToVariables;
    if (keyspaceName != null && tableName != null) {
      Metadata metadata = session.getCluster().getMetadata();
      KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
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

    // create indexed mappings only for unload, and only if the connector really requires it, to
    // match the order in which the query declares variables.
    if (mapping == null) {
      fieldsToVariables = inferFieldsToVariablesMap(columns, expectIndexedMapping);
    } else {
      if (mapping.hasPath(INFERRED_MAPPING_TOKEN)) {
        fieldsToVariables =
            inferFieldsToVariablesMap(
                new InferredMappingSpec(mapping.getValue(INFERRED_MAPPING_TOKEN)),
                columns,
                expectIndexedMapping);
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

    if (workflowType == LOAD) {
      validateAllKeysPresent(session, fieldsToVariables);
    }

    validateAllFieldsPresent(fieldsToVariables, columns);
    validateMappedRecords(fieldsToVariables, expectIndexedMapping);

    return fieldsToVariables;
  }

  private void validateMappedRecords(
      BiMap<String, String> fieldsToVariables, boolean expectIndexedMapping) {
    if (expectIndexedMapping && !containsIndexedMappings(fieldsToVariables)) {
      throw new BulkConfigurationException(
          "Schema mapping only contains named fields, but connector only supports indexed fields, "
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

  private void validateAllKeysPresent(Session session, BiMap<String, String> fieldsToVariables) {
    if (table == null) {
      assert preparedStatement != null;
      // infer table from bound variables
      ColumnDefinitions definitions = preparedStatement.getVariables();
      if (definitions != null && definitions.size() > 0) {
        table = inferTable(session, definitions);
      }
      if (table == null) {
        // infer table from result variables
        definitions = DriverCoreHooks.resultSetVariables(preparedStatement);
        if (definitions != null && definitions.size() > 0) {
          table = inferTable(session, definitions);
        }
      }
    }
    // table can only be null in rare cases e.g. if all variables are UDFs
    if (table != null) {
      List<ColumnMetadata> primaryKey = table.getPrimaryKey();
      primaryKey.forEach(
          key -> {
            if (!fieldsToVariables.containsValue(key.getName())) {
              throw new BulkConfigurationException(
                  "Missing required primary key column "
                      + Metadata.quoteIfNecessary(key.getName())
                      + " from schema.mapping or schema.query");
            }
          });
    }
  }

  private Config getMapping() throws BulkConfigurationException {
    mappingString = config.getString(MAPPING).replaceAll("\\*", INFERRED_MAPPING_TOKEN);
    try {
      return ConfigFactory.parseString(ConfigUtils.ensureBraces(mappingString));
    } catch (ConfigException.Parse e) {
      // mappingString doesn't seem to be a map. Treat it as a list instead.
      Map<String, String> indexMap = new LinkedHashMap<>();
      int curInd = 0;
      List<String> list =
          ConfigFactory.parseString(
                  "key = " + ConfigUtils.ensureBrackets(config.getString(MAPPING)))
              .getStringList("key");
      for (String s : list) {
        indexMap.put(Integer.toString(curInd++), s);
      }
      return ConfigFactory.parseMap(indexMap);
    }
  }

  private ImmutableBiMap<String, String> inferFieldsToVariablesMap(
      Supplier<List<String>> columns, boolean createIndexedMapping) {
    return inferFieldsToVariablesMap(null, columns, createIndexedMapping);
  }

  private ImmutableBiMap<String, String> inferFieldsToVariablesMap(
      InferredMappingSpec spec, Supplier<List<String>> columns, boolean createIndexedMapping) {

    // use a builder to preserve iteration order
    ImmutableBiMap.Builder<String, String> fieldsToVariables = new ImmutableBiMap.Builder<>();

    int i = 0;
    for (String colName : columns.get()) {
      if (spec == null || spec.allow(colName)) {
        // don't quote column names here, it will be done later on if required
        // for unload only, use the query's variable order
        if (createIndexedMapping) {
          fieldsToVariables.put(Integer.toString(i), colName);
        } else {
          fieldsToVariables.put(colName, colName);
        }
      }
      i++;
    }
    return fieldsToVariables.build();
  }

  private String inferWriteQuery(BiMap<String, String> fieldsToVariables) {
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
    CodePointCharStream input = CharStreams.fromString(query);
    CqlLexer lexer = new CqlLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    CqlParser parser = new CqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              Recognizer<?, ?> recognizer,
              Object offendingSymbol,
              int line,
              int charPositionInLine,
              String msg,
              RecognitionException e) {
            LOGGER.warn(
                "Supplied schema.query could not be parsed at line {}:{}: {}",
                line,
                charPositionInLine,
                msg);
          }
        });
    ParseTree query = parser.cqlStatement();
    ParseTreeWalker walker = new ParseTreeWalker();
    UsingTimestampListener listener = new UsingTimestampListener();
    walker.walk(listener, query);
    return listener.writeTimeVariable;
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

  private @NotNull Iterable<String> orderedMappingKeys() {
    // unfortunately typesafe config does not preserve iteration order of keys in a map :(
    // this is an ugly workaround
    List<String> orderedKeys =
        new ArrayList<>(mapping.withoutPath(INFERRED_MAPPING_TOKEN).root().keySet());
    orderedKeys.sort(Comparator.comparingInt(s -> mappingString.indexOf(s)));
    return orderedKeys;
  }

  @NotNull
  private static Set<String> maybeSortCols(BiMap<String, String> fieldsToVariables) {
    Set<String> cols;
    if (containsIndexedMappings(fieldsToVariables)) {
      // order columns by index
      cols = new TreeSet<>(fieldsToVariables.values());
    } else {
      // preserve original order of variables in the mapping
      cols = new LinkedHashSet<>(fieldsToVariables.values());
    }
    return cols;
  }

  private static boolean containsIndexedMappings(BiMap<String, String> fieldsToVariables) {
    return fieldsToVariables.keySet().stream().anyMatch(s -> s.matches("\\d+"));
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

  private static TableMetadata inferTable(Session session, ColumnDefinitions definitions) {
    String keyspaceName = definitions.getKeyspace(0);
    String tableName = definitions.getTable(0);
    KeyspaceMetadata keyspace =
        session.getCluster().getMetadata().getKeyspace(Metadata.quoteIfNecessary(keyspaceName));
    if (keyspace == null) {
      return null;
    }
    return keyspace.getTable(Metadata.quoteIfNecessary(tableName));
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

  private static class UsingTimestampListener extends CqlBaseListener {
    private String writeTimeVariable;

    @Override
    public void enterUsingTimestamp(CqlParser.UsingTimestampContext ctx) {
      if (ctx.getChildCount() > 1) {
        String text = ctx.getChild(1).getText();
        if (text.startsWith(":")) {
          writeTimeVariable = DriverCoreHooks.handleId(text.substring(1));
        }
      }
    }
  }

  private static class InferredMappingSpec {
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
