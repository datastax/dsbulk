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

import static com.datastax.oss.dsbulk.codecs.api.util.CodecUtils.instantToNumber;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.ALIASED_SELECTOR;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.INTERNAL;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.VARIABLE;
import static com.datastax.oss.dsbulk.mapping.MappingInspector.STAR;
import static com.datastax.oss.dsbulk.mapping.MappingInspector.TTL;
import static com.datastax.oss.dsbulk.mapping.MappingInspector.WRITETIME;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.INDEXED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_OR_INDEXED;
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
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLFragment;
import com.datastax.oss.dsbulk.mapping.CQLLiteral;
import com.datastax.oss.dsbulk.mapping.CQLRenderMode;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.DefaultMapping;
import com.datastax.oss.dsbulk.mapping.FunctionCall;
import com.datastax.oss.dsbulk.mapping.IndexedMappingField;
import com.datastax.oss.dsbulk.mapping.MappedMappingField;
import com.datastax.oss.dsbulk.mapping.Mapping;
import com.datastax.oss.dsbulk.mapping.MappingField;
import com.datastax.oss.dsbulk.mapping.MappingInspector;
import com.datastax.oss.dsbulk.mapping.MappingPreference;
import com.datastax.oss.dsbulk.mapping.TypedCQLLiteral;
import com.datastax.oss.dsbulk.partitioner.TokenRangeReadStatementGenerator;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.DefaultRecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.NestedBatchException;
import com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode;
import com.datastax.oss.dsbulk.workflow.commons.utils.GraphUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
  private static final String PRESERVE_TIMESTAMP = "preserveTimestamp";
  private static final String PRESERVE_TTL = "preserveTtl";
  private static final String CORE = "Core";
  private static final String SPLITS = "splits";

  private static final Predicate<FunctionCall> WRITETIME_OR_TTL =
      fc -> fc.getFunctionName().equals(WRITETIME) || fc.getFunctionName().equals(TTL);

  private final Config config;
  private final SchemaGenerationStrategy schemaGenerationStrategy;

  private boolean nullToUnset;
  private boolean allowExtraFields;
  private boolean allowMissingFields;
  private int splits;
  private MappingInspector mapping;
  private int ttlSeconds;
  private long timestampMicros;
  private boolean preserveTimestamp;
  private boolean preserveTtl;
  private RelationMetadata table;
  private KeyspaceMetadata keyspace;
  private CQLWord keyspaceName;
  private CQLWord tableName;
  private String query;
  private QueryInspector queryInspector;
  private List<PreparedStatement> preparedStatements;
  private MappingPreference mappingPreference;
  private ConvertingCodecFactory codecFactory;

  public SchemaSettings(Config config, SchemaGenerationStrategy schemaGenerationStrategy) {
    this.config = config;
    this.schemaGenerationStrategy = schemaGenerationStrategy;
  }

  public void init(
      CqlSession session,
      ConvertingCodecFactory codecFactory,
      boolean indexedMappingSupported,
      boolean mappedMappingSupported) {

    this.codecFactory = codecFactory;

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

      // Keyspace

      if (config.hasPath(KEYSPACE)) {
        keyspace = locateKeyspace(session.getMetadata(), config.getString(KEYSPACE));
      } else if (config.hasPath(GRAPH)) {
        keyspace = locateKeyspace(session.getMetadata(), config.getString(GRAPH));
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
          ConvertingCodec<String, Instant> codec =
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, true);
          Instant instant = codec.externalToInternal(timestampStr);
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

      preserveTimestamp = config.getBoolean(PRESERVE_TIMESTAMP);
      preserveTtl = config.getBoolean(PRESERVE_TTL);

      // Custom Query

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

        if (preserveTimestamp || preserveTtl) {
          throw new IllegalArgumentException(
              "Setting schema.query must not be defined if schema.preserveTimestamp or schema.preserveTtl is defined");
        }

      } else {

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
      } else if (schemaGenerationStrategy.isMapping()) {
        throw new IllegalArgumentException(
            "Connector must support at least one of indexed or mapped mappings");
      }

      if (config.hasPath(MAPPING)) {

        if (!schemaGenerationStrategy.isMapping()) {
          throw new IllegalArgumentException(
              "Setting schema.mapping must not be defined when counting rows in a table");
        }

        Supplier<CQLWord> usingTimestampVariable = null;
        Supplier<CQLWord> usingTTLVariable = null;
        if (queryInspector != null) {
          usingTimestampVariable = queryInspector.getUsingTimestampVariable()::get;
          usingTTLVariable = queryInspector.getUsingTTLVariable()::get;
        }

        // TODO remove support for providing external variable names for the deprecated
        // __ttl and __timestamp mapping tokens.
        @SuppressWarnings("deprecation")
        MappingInspector mapping =
            new MappingInspector(
                config.getString(MAPPING),
                schemaGenerationStrategy.isWriting(),
                mappingPreference,
                usingTimestampVariable,
                usingTTLVariable);
        this.mapping = mapping;

        Set<MappingField> fields = mapping.getExplicitMappings().keySet();
        Collection<CQLFragment> variables = mapping.getExplicitMappings().values();

        if (schemaGenerationStrategy.isWriting()) {
          // f1 = now() never allowed when loading
          // f1 = writetime(*) only allowed if schema.query not present
          // now() = c1 only allowed if schema.query not present
          if (containsFunctionCalls(variables, WRITETIME_OR_TTL.negate())) {
            throw new IllegalArgumentException(
                "Misplaced function call detected on the right side of a mapping entry; "
                    + "please review your schema.mapping setting");
          }
          if (query != null && containsFunctionCalls(variables, WRITETIME_OR_TTL)) {
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when loading if schema.mapping "
                    + "contains a writetime or ttl function on the right side of a mapping entry");
          }
          if (query != null && containsFunctionCalls(fields)) {
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when loading if schema.mapping "
                    + "contains a function on the left side of a mapping entry");
          }
          if (containsWritetimeOrTTLFunctionCalls(mapping.getExplicitMappings())) {
            throw new IllegalArgumentException(
                "Misplaced function call detected on the left side of a writetime or TTL mapping entry; "
                    + "please review your schema.mapping setting");
          }
          // f1 = (text)'abc' never allowed when loading
          // (text)'abc' = c1 only allowed if schema.query not present
          if (containsConstantExpressions(variables)) {
            throw new IllegalArgumentException(
                "Misplaced constant expression detected on the right side of a mapping entry; "
                    + "please review your schema.mapping setting");
          }
          if (query != null && containsConstantExpressions(fields)) {
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when loading if schema.mapping "
                    + "contains a constant expression on the left side of a mapping entry");
          }
        }

        if (schemaGenerationStrategy.isReading()) {
          // now() = c1 never allowed when unloading
          // f1 = now() only allowed if schema.query not present
          if (containsFunctionCalls(fields)) {
            throw new IllegalArgumentException(
                "Misplaced function call detected on the left side of a mapping entry; "
                    + "please review your schema.mapping setting");
          }
          if (query != null && containsFunctionCalls(variables)) {
            throw new IllegalArgumentException(
                "Setting schema.query must not be defined when unloading if schema.mapping "
                    + "contains a function on the right side of a mapping entry");
          }
          // (text)'abc' = c1 never allowed when unloading
          // f1 = (text)'abc' only allowed if schema.query not present and literal selectors
          // supported
          if (containsConstantExpressions(fields)) {
            throw new IllegalArgumentException(
                "Misplaced constant expression detected on the left side of a mapping entry; "
                    + "please review your schema.mapping setting");
          }
          if (containsConstantExpressions(variables)) {
            if (query != null) {
              throw new IllegalArgumentException(
                  "Setting schema.query must not be defined when unloading if schema.mapping "
                      + "contains a constant expression on the right side of a mapping entry");
            }
            if (!checkLiteralSelectorsSupported(session)) {
              throw new IllegalStateException(
                  "At least one constant expression appears on the right side of a mapping entry, "
                      + "but the cluster does not support CQL literals in the SELECT clause; "
                      + " please review your schema.mapping setting");
            }
          }
        }

        if ((preserveTimestamp || preserveTtl) && !mapping.isInferring()) {
          throw new IllegalStateException(
              "Setting schema.mapping must contain an inferring entry (e.g. '*=*') "
                  + "when schema.preserveTimestamp or schema.preserveTtl is enabled");
        }

      } else {

        mapping =
            new MappingInspector("*=*", schemaGenerationStrategy.isWriting(), mappingPreference);
      }

      // Misc

      nullToUnset = config.getBoolean(NULL_TO_UNSET);
      allowExtraFields = config.getBoolean(ALLOW_EXTRA_FIELDS);
      allowMissingFields = config.getBoolean(ALLOW_MISSING_FIELDS);
      splits = ConfigUtils.getThreads(config, SPLITS);

      // Final checks related to graph operations

      if (hasGraphOptions(config)) {

        GraphUtils.checkGraphCompatibility(session);

        if (!isGraph(keyspace)) {
          throw new IllegalStateException(
              "Graph operations requested but provided keyspace is not a graph: " + keyspaceName);
        }
        if (!isSupportedGraph(keyspace)) {
          assert ((DseGraphKeyspaceMetadata) keyspace).getGraphEngine().isPresent();
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
          if (schemaGenerationStrategy == SchemaGenerationStrategy.MAP_AND_WRITE) {
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
      CqlSession session, RecordMetadata recordMetadata, boolean batchingEnabled)
      throws IllegalArgumentException {
    if (!schemaGenerationStrategy.isWriting() || !schemaGenerationStrategy.isMapping()) {
      throw new IllegalStateException(
          "Cannot create record mapper when schema generation strategy is "
              + schemaGenerationStrategy);
    }
    Mapping mapping =
        prepareStatementAndCreateMapping(
            session, batchingEnabled, EnumSet.noneOf(StatisticsMode.class));
    ProtocolVersion protocolVersion = session.getContext().getProtocolVersion();
    if (protocolVersion.getCode() < DefaultProtocolVersion.V4.getCode() && nullToUnset) {
      LOGGER.warn(
          String.format(
              "Protocol version in use (%s) does not support unset bound variables; "
                  + "forcing schema.nullToUnset to false",
              protocolVersion));
      nullToUnset = false;
    }
    return new DefaultRecordMapper(
        preparedStatements,
        partitionKeyVariables(),
        mutatesOnlyStaticColumns() ? Collections.emptySet() : clusteringColumnVariables(),
        protocolVersion,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields);
  }

  /**
   * Creates a new {@link ReadResultMapper}.
   *
   * @param session The session to use when preparing the SELECT statement.
   * @param recordMetadata The {@link RecordMetadata} to use for column mappings.
   * @param codecFactory The {@link ConvertingCodecFactory} to use to locate codecs for the mapping
   *     operation.
   * @param retainRecordSources Whether the mapper should retain record sources; if {@code true},
   *     all emitted records will contain the original row as {@linkplain Record#getSource() their
   *     sources}.
   */
  public ReadResultMapper createReadResultMapper(
      CqlSession session,
      RecordMetadata recordMetadata,
      ConvertingCodecFactory codecFactory,
      boolean retainRecordSources) {
    if (!schemaGenerationStrategy.isReading() || !schemaGenerationStrategy.isMapping()) {
      throw new IllegalStateException(
          "Cannot create read result mapper when schema generation strategy is "
              + schemaGenerationStrategy);
    }
    // we don't check that mapping records are supported when unloading, the only thing that matters
    // is the order in which fields appear in the record.
    Mapping mapping =
        prepareStatementAndCreateMapping(session, false, EnumSet.noneOf(StatisticsMode.class));
    return new DefaultReadResultMapper(
        mapping, recordMetadata, getTargetTableURI(), retainRecordSources);
  }

  public ReadResultCounter createReadResultCounter(
      CqlSession session,
      ConvertingCodecFactory codecFactory,
      EnumSet<StatsSettings.StatisticsMode> modes,
      int numPartitions) {
    if (!schemaGenerationStrategy.isReading() || !schemaGenerationStrategy.isCounting()) {
      throw new IllegalStateException(
          "Cannot create read result counter when schema generation strategy is "
              + schemaGenerationStrategy);
    }
    prepareStatementAndCreateMapping(session, false, modes);
    if (modes.contains(StatisticsMode.partitions) && table.getClusteringColumns().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot count partitions for table %s: it has no clustering column.",
              tableName.render(VARIABLE)));
    }
    return new DefaultReadResultCounter(
        keyspace.getName(),
        session.getMetadata(),
        modes,
        numPartitions,
        session.getContext().getProtocolVersion(),
        codecFactory);
  }

  public List<Statement<?>> createReadStatements(CqlSession session) {
    PreparedStatement preparedStatement = preparedStatements.get(0);
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
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    List<Statement<?>> statements =
        generator.generate(
            splits,
            range ->
                preparedStatement
                    .bind()
                    .setToken(
                        queryInspector.getTokenRangeRestrictionStartVariableIndex(),
                        range.getStart())
                    .setToken(
                        queryInspector.getTokenRangeRestrictionEndVariableIndex(), range.getEnd()));

    LOGGER.debug("Generated {} bound statements", statements.size());
    // Shuffle the statements to avoid hitting the same replicas sequentially when
    // the statements will be executed.
    Collections.shuffle(statements);
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

  /**
   * Returns a "resource URI" identifying the operation's target table. Suitable to be used as the
   * resource URI of records created by a {@link ReadResultMapper} targeting that same table.
   */
  @NonNull
  @VisibleForTesting
  URI getTargetTableURI() {
    // Contrary to other identifiers, keyspace and table names MUST contain only alpha-numeric
    // characters and underscores. So there is no need to escape path segments in the resulting URI.
    return URI.create(
        "cql://" + keyspace.getName().asInternal() + '/' + table.getName().asInternal());
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
  private Mapping prepareStatementAndCreateMapping(
      CqlSession session, boolean batchingEnabled, EnumSet<StatisticsMode> modes) {
    ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables = null;
    if (!config.hasPath(QUERY)) {
      // in the absence of user-provided queries, create the mapping *before* query generation and
      // preparation
      List<CQLFragment> columns =
          table.getColumns().values().stream()
              .filter(col -> !isDSESearchPseudoColumn(col))
              .flatMap(
                  column -> {
                    CQLWord colName = CQLWord.fromCqlIdentifier(column.getName());
                    List<CQLFragment> cols = Lists.newArrayList(colName);
                    if (schemaGenerationStrategy.isMapping()) {
                      if (preserveTimestamp && checkWritetimeTtlSupported(column, WRITETIME)) {
                        cols.add(new FunctionCall(null, WRITETIME, colName));
                      }
                      if (preserveTtl && checkWritetimeTtlSupported(column, TTL)) {
                        cols.add(new FunctionCall(null, TTL, colName));
                      }
                    }
                    return cols.stream();
                  })
              .collect(Collectors.toList());
      fieldsToVariables = createFieldsToVariablesMap(columns);
      // query generation
      if (schemaGenerationStrategy.isWriting()) {
        if (isCounterTable()) {
          query = inferUpdateCounterQuery(fieldsToVariables);
        } else if (requiresBatchInsertQuery(fieldsToVariables)) {
          query = inferBatchInsertQuery(fieldsToVariables);
        } else {
          query = inferInsertQuery(fieldsToVariables);
        }
      } else if (schemaGenerationStrategy.isReading() && schemaGenerationStrategy.isMapping()) {
        query = inferReadQuery(fieldsToVariables);
      } else if (schemaGenerationStrategy.isReading() && schemaGenerationStrategy.isCounting()) {
        query = inferCountQuery(modes);
      } else {
        throw new IllegalStateException(
            "Unsupported schema generation strategy: " + schemaGenerationStrategy);
      }
      LOGGER.debug("Inferred query: {}", query);
      queryInspector = new QueryInspector(query);
      // validate generated query
      if (schemaGenerationStrategy.isWriting()) {
        validatePrimaryKeyPresent(fieldsToVariables);
      }
    }
    assert query != null;
    assert queryInspector != null;
    if (!queryInspector.getKeyspaceName().isPresent()) {
      session.execute("USE " + keyspaceName);
    }
    // Transform user-provided queries before preparation
    if (config.hasPath(QUERY)) {
      if (schemaGenerationStrategy.isReading() && queryInspector.isParallelizable()) {
        int whereClauseIndex = queryInspector.getFromClauseEndIndex() + 1;
        StringBuilder sb = new StringBuilder(query.substring(0, whereClauseIndex));
        appendTokenRangeRestriction(sb);
        query = sb.append(query.substring(whereClauseIndex)).toString();
      }
      if (schemaGenerationStrategy.isCounting()) {
        if (modes.contains(StatisticsMode.partitions)
            || modes.contains(StatisticsMode.ranges)
            || modes.contains(StatisticsMode.hosts)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot count with stats.modes = %s when schema.query is provided; "
                      + "only stats.modes = [global] is allowed",
                  modes));
        }
        // reduce row size by only selecting one column
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(getGlobalCountSelector());
        query =
            sb.append(' ')
                .append(query.substring(queryInspector.getFromClauseStartIndex()))
                .toString();
      }
      queryInspector = new QueryInspector(query);
    }
    if (batchingEnabled && queryInspector.isBatch()) {
      preparedStatements = unwrapAndPrepareBatchChildStatements(session);
    } else {
      preparedStatements = Collections.singletonList(session.prepare(query));
    }
    if (config.hasPath(QUERY)) {
      // in the presence of user-provided queries, create the mapping *after* query preparation
      Stream<ColumnDefinitions> variables = getVariables();
      fieldsToVariables =
          createFieldsToVariablesMap(
              variables
                  .flatMap(defs -> StreamSupport.stream(defs.spliterator(), false))
                  .map(def -> def.getName().asInternal())
                  .map(CQLWord::fromInternal)
                  .collect(Collectors.toList()));
      // validate user-provided query
      if (schemaGenerationStrategy.isWriting()) {
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
        transformFieldsToVariables(fieldsToVariables),
        codecFactory,
        transformWriteTimeVariables(queryInspector.getWriteTimeVariables()));
  }

  private ImmutableList<PreparedStatement> unwrapAndPrepareBatchChildStatements(
      CqlSession session) {
    if (queryInspector.getBatchType().filter(t -> t != BatchType.UNLOGGED).isPresent()) {
      throw new NestedBatchException(
          String.format(
              "Batching cannot be enabled when the prepared query is a BATCH of type %s; "
                  + "forcibly disabling batching. "
                  + "To improve performance, consider using UNLOGGED batches if possible. "
                  + "To suppress this warning, set batch.mode to DISABLED.",
              queryInspector.getBatchType().get()));
    } else if (queryInspector.hasBatchLevelUsingClause()) {
      throw new NestedBatchException(
          "Batching cannot be enabled when the prepared query is a BATCH with a batch-level USING clause; "
              + "forcibly disabling batching. "
              + "To improve performance, consider defining child-level USING clauses instead. "
              + "To suppress this warning, set batch.mode to DISABLED.");
    } else if (config.hasPath(QUERY)) {
      LOGGER.info(
          "Batching is enabled: to improve performance, the original BATCH query will be unwrapped "
              + "and each child statement will be prepared independently.");
    }
    ImmutableList.Builder<PreparedStatement> builder = ImmutableList.builder();
    for (String childStatement : queryInspector.getBatchChildStatements()) {
      builder.add(session.prepare(childStatement));
    }
    return builder.build();
  }

  private boolean checkWritetimeTtlSupported(ColumnMetadata col, CQLWord functionName) {
    if (isCounterTable() || table.getPrimaryKey().contains(col)) {
      // Counter tables and primary key columns don't have timestamps and TTLs.
      return false;
    }
    boolean supported;
    if (col.getType() instanceof ListType
        || col.getType() instanceof SetType
        || col.getType() instanceof MapType) {
      // Collection support for WRITETIME and TTL varies depending on the Cassandra and DSE versions
      // in use, but in all cases, even if the export is possible, it wouldn't be possible to import
      // the data back to Cassandra. This is valid for frozen collections as well.
      supported = false;
    } else if (col.getType() instanceof UserDefinedType) {
      // While most C* versions (all?) accept WRITETIME and TTL functions on UDTs, the functions
      // (always?) return NULL for non-frozen ones. Therefore, we only consider frozen UDTs as
      // supported. Also note that all tuples are accepted, since they are frozen by nature.
      supported = ((UserDefinedType) col.getType()).isFrozen();
    } else {
      supported = true;
    }
    if (!supported) {
      LOGGER.warn(
          "Skipping {} preservation for column {}: this feature is not supported for CQL type {}.",
          Objects.equals(functionName, WRITETIME) ? "timestamp" : "TTL",
          col.getName().asCql(true),
          col.getType().asCql(true, true));
    }
    return supported;
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

  private Stream<ColumnDefinitions> getVariables() {
    if (schemaGenerationStrategy.isWriting()) {
      return preparedStatements.stream().map(PreparedStatement::getVariableDefinitions);
    } else {
      assert schemaGenerationStrategy.isReading();
      return preparedStatements.stream().map(PreparedStatement::getResultSetDefinitions);
    }
  }

  private ImmutableMultimap<MappingField, CQLFragment> createFieldsToVariablesMap(
      Collection<CQLFragment> columnsOrVariables) throws IllegalArgumentException {
    ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables;
    if (mapping.isInferring()) {
      fieldsToVariables = inferFieldsToVariablesMap(columnsOrVariables);
    } else {
      fieldsToVariables = ImmutableMultimap.of();
    }

    ImmutableMultimap<MappingField, CQLFragment> explicitMappings = mapping.getExplicitMappings();
    if (!explicitMappings.isEmpty()) {
      // Take into account all explicit mappings
      ImmutableMultimap.Builder<MappingField, CQLFragment> builder = ImmutableMultimap.builder();
      for (Map.Entry<MappingField, CQLFragment> entry : explicitMappings.entries()) {
        builder.put(entry.getKey(), entry.getValue());
      }
      // Take into account only inferred mappings that contain fields and variables not referenced
      // by any explicit mapping
      for (Map.Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
        if (!explicitMappings.containsKey(entry.getKey())
            && !explicitMappings.containsValue(entry.getValue())) {
          builder.put(entry.getKey(), entry.getValue());
        }
      }
      fieldsToVariables = builder.build();
    }

    Preconditions.checkState(
        !fieldsToVariables.isEmpty(),
        "Mapping was absent and could not be inferred, please provide an explicit mapping");

    validateAllFieldsPresent(fieldsToVariables, columnsOrVariables);
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
  private RelationMetadata locateTable(KeyspaceMetadata keyspace, String tableNameInternal) {
    CqlIdentifier tableName = CqlIdentifier.fromInternal(tableNameInternal);
    RelationMetadata table = keyspace.getTable(tableName).orElse(null);
    if (table == null) {
      if (schemaGenerationStrategy.isReading()) {
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
          if (value instanceof CQLWord && !columns.contains(value)) {
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
      CQLWord pkVariable = CQLWord.fromCqlIdentifier(pk.getName());
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

    List<CQLFragment> excludedVariables = new ArrayList<>(mapping.getExcludedVariables());

    if (!isCounterTable() && schemaGenerationStrategy.isMapping()) {
      for (CQLWord variable : mapping.getExcludedVariables()) {
        if (preserveTimestamp) {
          excludedVariables.add(new FunctionCall(null, WRITETIME, variable));
        }
        if (preserveTtl) {
          excludedVariables.add(new FunctionCall(null, TTL, variable));
        }
      }
    }

    int i = 0;
    for (CQLFragment colName : columns) {
      if (!excludedVariables.contains(colName)) {
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
    ImmutableMultimap.Builder<MappingField, CQLFragment> regularFieldsToVariablesBuilder =
        ImmutableMultimap.builder();
    CQLFragment writetime = null;
    CQLFragment ttl = null;
    for (Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      if (entry.getValue() instanceof FunctionCall) {
        FunctionCall functionCall = (FunctionCall) entry.getValue();
        if (functionCall.getFunctionName().equals(WRITETIME)) {
          assert writetime == null;
          if (entry.getKey() instanceof CQLLiteral) {
            writetime = (CQLLiteral) entry.getKey();
          } else {
            writetime = CQLWord.fromInternal(functionCall.render(INTERNAL));
          }
        } else if (functionCall.getFunctionName().equals(TTL)) {
          assert ttl == null;
          if (entry.getKey() instanceof CQLLiteral) {
            ttl = (CQLLiteral) entry.getKey();
          } else {
            ttl = CQLWord.fromInternal(functionCall.render(INTERNAL));
          }
        }
      } else {
        regularFieldsToVariablesBuilder.put(entry);
      }
    }
    ImmutableMultimap<MappingField, CQLFragment> regularFieldsToVariables =
        regularFieldsToVariablesBuilder.build();
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspaceName.render(VARIABLE))
        .append('.')
        .append(tableName.render(VARIABLE))
        .append(" (");
    appendColumnNames(regularFieldsToVariables, sb, VARIABLE);
    sb.append(") VALUES (");
    Set<CQLFragment> cols = maybeSortCols(regularFieldsToVariables);
    Iterator<CQLFragment> it = cols.iterator();
    while (it.hasNext()) {
      CQLFragment col = it.next();
      // for insert queries there can be only one field mapped to a given column
      MappingField field = fieldsToVariables.inverse().get(col).iterator().next();
      if (field instanceof CQLFragment) {
        sb.append(((CQLFragment) field).render(NAMED_ASSIGNMENT));
      } else {
        sb.append(col.render(NAMED_ASSIGNMENT));
      }
      if (it.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append(')');
    appendWriteTimeAndTTL(sb, writetime, ttl);
    return sb.toString();
  }

  private static class WriteTimeAndTTL {
    private CQLFragment value;
    private CQLFragment writetime;
    private CQLFragment ttl;
  }

  private String inferBatchInsertQuery(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    List<CQLWord> pks = primaryKeyColumns();
    Set<CQLFragment> allSpecificVariables = new LinkedHashSet<>();
    Map<CQLWord, WriteTimeAndTTL> specificWriteTimesAndTTLs = new LinkedHashMap<>();
    boolean hasGlobalWritetime = false;
    boolean hasGlobalTTL = false;
    for (CQLFragment variable : fieldsToVariables.values()) {
      if (variable instanceof FunctionCall) {
        FunctionCall functionCall = (FunctionCall) variable;
        if (functionCall.getFunctionName().equals(WRITETIME)) {
          for (CQLFragment arg : functionCall.getArgs()) {
            if (arg.equals(STAR)) {
              if (preserveTimestamp) {
                throw new IllegalStateException(
                    "Invalid mapping: writetime(*) is not allowed when schema.preserveTimestamp is true.");
              }
              hasGlobalWritetime = true;
            } else {
              CQLWord col = (CQLWord) arg;
              if (pks.contains(col)) {
                throw new IllegalStateException(
                    "Invalid mapping: writetime() function arg must be either '*' or a non-primary key column name.");
              }
              if (fieldsToVariables.containsValue(col)) {
                allSpecificVariables.add(col);
                allSpecificVariables.add(functionCall);
                specificWriteTimesAndTTLs.compute(
                    col,
                    (k, v) -> {
                      if (v == null) {
                        v = new WriteTimeAndTTL();
                        MappingField colField =
                            fieldsToVariables.inverse().get(col).iterator().next();
                        v.value = colField instanceof CQLFragment ? (CQLFragment) colField : col;
                      }
                      MappingField writetimeField =
                          fieldsToVariables.inverse().get(functionCall).iterator().next();
                      v.writetime =
                          writetimeField instanceof CQLLiteral
                              ? (CQLLiteral) writetimeField
                              : CQLWord.fromInternal(functionCall.render(INTERNAL));
                      return v;
                    });
              } else {
                throw new IllegalStateException(
                    String.format(
                        "Invalid mapping: target column %s must be present if %s is also present.",
                        col.render(VARIABLE), functionCall.render(INTERNAL)));
              }
            }
          }
        } else if (functionCall.getFunctionName().equals(TTL)) {
          for (CQLFragment arg : functionCall.getArgs()) {
            if (arg.equals(STAR)) {
              if (preserveTtl) {
                throw new IllegalStateException(
                    "Invalid mapping: ttl(*) is not allowed when schema.preserveTtl is true.");
              }
              hasGlobalTTL = true;
            } else {
              CQLWord col = (CQLWord) arg;
              if (pks.contains(col)) {
                throw new IllegalStateException(
                    "Invalid mapping: ttl() function arg must be either '*' or a non-primary key column name.");
              }
              if (fieldsToVariables.containsValue(col)) {
                allSpecificVariables.add(col);
                allSpecificVariables.add(functionCall);
                specificWriteTimesAndTTLs.compute(
                    (CQLWord) arg,
                    (k, v) -> {
                      if (v == null) {
                        v = new WriteTimeAndTTL();
                        MappingField colField =
                            fieldsToVariables.inverse().get(col).iterator().next();
                        v.value = colField instanceof CQLFragment ? (CQLFragment) colField : col;
                      }
                      MappingField ttlField =
                          fieldsToVariables.inverse().get(functionCall).iterator().next();
                      v.ttl =
                          ttlField instanceof CQLLiteral
                              ? (CQLLiteral) ttlField
                              : CQLWord.fromInternal(functionCall.render(INTERNAL));
                      return v;
                    });
              } else {
                throw new IllegalStateException(
                    String.format(
                        "Invalid mapping: target column %s must be present if %s is also present.",
                        col.render(VARIABLE), functionCall.render(INTERNAL)));
              }
            }
          }
        }
      }
    }

    ImmutableMultimap.Builder<MappingField, CQLFragment> defaultFieldsToVariablesBuilder =
        ImmutableMultimap.builder();
    for (Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      CQLFragment variable = entry.getValue();
      if (!allSpecificVariables.contains(variable)) {
        defaultFieldsToVariablesBuilder.put(entry);
      }
    }
    ImmutableMultimap<MappingField, CQLFragment> defaultFieldsToVariables =
        defaultFieldsToVariablesBuilder.build();

    boolean hasRegularColumnsWithoutSpecificWritetimeAndTTL =
        defaultFieldsToVariables.values().stream()
            .filter(CQLWord.class::isInstance)
            .map(CQLWord.class::cast)
            .anyMatch(variable -> !pks.contains(variable));

    if (!hasRegularColumnsWithoutSpecificWritetimeAndTTL) {
      if (hasGlobalWritetime) {
        throw new IllegalStateException(
            "Invalid mapping: writetime(*) function has no target column.");
      }
      if (hasGlobalTTL) {
        throw new IllegalStateException("Invalid mapping: ttl(*) function has no target column.");
      }
    }

    StringBuilder sb = new StringBuilder();
    if (!hasRegularColumnsWithoutSpecificWritetimeAndTTL && specificWriteTimesAndTTLs.size() == 1) {
      // edge case: there is only one regular column in the table,
      // and it has specific writetime or tll: no need for a BATCH as there is only one child
      // statement.
      Entry<CQLWord, WriteTimeAndTTL> entry =
          specificWriteTimesAndTTLs.entrySet().iterator().next();
      appendBatchChildQuery(
          sb,
          entry.getKey(),
          entry.getValue().value,
          entry.getValue().writetime,
          entry.getValue().ttl,
          pks);
    } else {
      sb.append("BEGIN UNLOGGED BATCH ");
      // if there are any variables not assigned to specific TTL or writetime function calls,
      // generate a first INSERT INTO child query similar to the ones generated for simple INSERTs.
      if (hasRegularColumnsWithoutSpecificWritetimeAndTTL) {
        sb.append(inferInsertQuery(defaultFieldsToVariables)).append("; ");
      }
      // for all variables having specific TTLs and/or writetimes,
      // generate a specific INSERT INTO query for that variable only + its TTL and/or writetime.
      for (Entry<CQLWord, WriteTimeAndTTL> entry : specificWriteTimesAndTTLs.entrySet()) {
        appendBatchChildQuery(
            sb,
            entry.getKey(),
            entry.getValue().value,
            entry.getValue().writetime,
            entry.getValue().ttl,
            pks);
        sb.append("; ");
      }
      sb.append("APPLY BATCH");
    }
    return sb.toString();
  }

  private void appendBatchChildQuery(
      StringBuilder sb,
      CQLWord variable,
      CQLFragment value,
      @Nullable CQLFragment writetime,
      @Nullable CQLFragment ttl,
      List<CQLWord> pks) {
    sb.append("INSERT INTO ")
        .append(keyspaceName.render(VARIABLE))
        .append('.')
        .append(tableName.render(VARIABLE))
        .append(" (");
    for (CQLWord pk : pks) {
      sb.append(pk.render(VARIABLE));
      sb.append(", ");
    }
    sb.append(variable.render(VARIABLE)).append(") VALUES (");
    for (CQLWord pk : pks) {
      sb.append(pk.render(NAMED_ASSIGNMENT));
      sb.append(", ");
    }
    sb.append(value.render(NAMED_ASSIGNMENT)).append(")");
    appendWriteTimeAndTTL(sb, writetime, ttl);
  }

  private String inferUpdateCounterQuery(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(keyspaceName.render(VARIABLE)).append('.').append(tableName.render(VARIABLE));
    // Note: TTL and timestamp are not allowed in counter queries;
    // a test is made inside the following method for fixed TTL and timestamps;
    // function-style TTL and timestamps will be tested below and forbidden as well
    appendWriteTimeAndTTL(sb, null, null);
    sb.append(" SET ");
    Set<CQLFragment> cols = maybeSortCols(fieldsToVariables);
    Iterator<CQLFragment> colsIterator = cols.iterator();
    boolean isFirst = true;
    List<CQLWord> pks = primaryKeyColumns();
    while (colsIterator.hasNext()) {
      CQLFragment col = colsIterator.next();
      if (col instanceof CQLWord && pks.contains(col)) {
        continue;
      }
      if (col instanceof FunctionCall || col instanceof CQLLiteral) {
        throw new IllegalArgumentException(
            "Invalid mapping: function calls are not allowed when updating a counter table.");
      }
      // for update queries there can be only one field mapped to a given column
      MappingField field = fieldsToVariables.inverse().get(col).iterator().next();
      if (field instanceof FunctionCall) {
        throw new IllegalArgumentException(
            "Invalid mapping: function calls are not allowed when updating a counter table.");
      } else if (field instanceof CQLLiteral) {
        throw new IllegalArgumentException(
            "Invalid mapping: constant expressions are not allowed when updating a counter table.");
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
    Iterator<CQLWord> pksIterator = pks.iterator();
    while (pksIterator.hasNext()) {
      CQLFragment col = pksIterator.next();
      sb.append(col.render(VARIABLE)).append(" = ").append(col.render(NAMED_ASSIGNMENT));
      if (pksIterator.hasNext()) {
        sb.append(" AND ");
      }
    }
    return sb.toString();
  }

  private void appendWriteTimeAndTTL(
      StringBuilder sb, @Nullable CQLFragment writetime, @Nullable CQLFragment ttl) {
    boolean hasTtl = ttlSeconds != -1 || ttl != null;
    boolean hasTimestamp = timestampMicros != -1 || writetime != null;
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
          assert ttl != null;
          sb.append(ttl.render(NAMED_ASSIGNMENT));
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
          assert writetime != null;
          if (writetime instanceof TypedCQLLiteral) {
            DataType dataType = ((TypedCQLLiteral) writetime).getDataType();
            if (dataType == DataTypes.TIMESTAMP) {
              ConvertingCodec<String, Instant> codec =
                  codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, true);
              String literal = ((CQLLiteral) writetime).getLiteral();
              Instant i = codec.externalToInternal(codec.parse(literal));
              long micros = instantToNumber(i, MICROSECONDS, EPOCH);
              writetime = new TypedCQLLiteral(Long.toString(micros), DataTypes.BIGINT);
            }
          }
          sb.append(writetime.render(NAMED_ASSIGNMENT));
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

  @NonNull
  private List<CQLWord> primaryKeyColumns() {
    return table.getPrimaryKey().stream()
        .map(ColumnMetadata::getName)
        .map(CQLWord::fromCqlIdentifier)
        .collect(Collectors.toList());
  }

  @NonNull
  private Set<CQLWord> partitionKeyVariables() {
    return columnsToVariables(table.getPartitionKey());
  }

  @NonNull
  private Set<CQLWord> clusteringColumnVariables() {
    return columnsToVariables(table.getClusteringColumns().keySet());
  }

  @NonNull
  private Set<CQLWord> columnsToVariables(Collection<ColumnMetadata> columns) {
    Map<CQLWord, CQLFragment> boundVariables = queryInspector.getAssignments();
    Set<CQLWord> variables = new HashSet<>(columns.size());
    for (ColumnMetadata column : columns) {
      CQLFragment variable = boundVariables.get(CQLWord.fromCqlIdentifier(column.getName()));
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
    while (it.hasNext()) {
      // this assumes that the variable name found in the mapping
      // corresponds to a CQL column having the exact same name.
      CQLFragment col = it.next();
      sb.append(col.render(mode));
      if (it.hasNext()) {
        sb.append(", ");
      }
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

  private static boolean requiresBatchInsertQuery(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    // a BATCH query is considered required if the mapping contains at least one non-global
    // writetime or ttl function call; edge cases will be handled in inferBatchInsertQuery.
    Optional<FunctionCall> nonGlobalWriteTimes =
        fieldsToVariables.values().stream()
            .filter(FunctionCall.class::isInstance)
            .map(FunctionCall.class::cast)
            .filter(fc -> fc.getFunctionName().equals(WRITETIME))
            .filter(fc -> !fc.getArgs().contains(STAR))
            .findAny();
    if (nonGlobalWriteTimes.isPresent()) {
      return true;
    } else {
      Optional<FunctionCall> nonGlobalTTLs =
          fieldsToVariables.values().stream()
              .filter(FunctionCall.class::isInstance)
              .map(FunctionCall.class::cast)
              .filter(fc -> fc.getFunctionName().equals(TTL))
              .filter(fc -> !fc.getArgs().contains(STAR))
              .findAny();
      return nonGlobalTTLs.isPresent();
    }
  }

  @NonNull
  private static ImmutableSetMultimap<Field, CQLWord> transformFieldsToVariables(
      ImmutableMultimap<MappingField, CQLFragment> fieldsToVariables) {
    ImmutableSetMultimap.Builder<Field, CQLWord> builder = ImmutableSetMultimap.builder();
    for (Entry<MappingField, CQLFragment> entry : fieldsToVariables.entries()) {
      if (entry.getKey() instanceof FunctionCall || entry.getKey() instanceof CQLLiteral) {
        // functions in fields should not be included in the final mapping since
        // the generated query includes the function call as is.
        continue;
      }
      // all variables must be included in the final mapping;
      // function calls must be transformed here into a CQL word, e.g.
      // plus(col1,col2) will generate a variable named "plus(col1, col2)".
      // This relies on creating names using the same logic that Cassandra uses
      // server-side to create variable names from function calls.
      builder.put(entry.getKey(), toCQLWord(entry.getValue()));
    }
    return builder.build();
  }

  @NonNull
  private static ImmutableSet<CQLWord> transformWriteTimeVariables(
      ImmutableSet<CQLFragment> writeTimeVariables) {
    ImmutableSet.Builder<CQLWord> builder = ImmutableSet.builder();
    for (CQLFragment fragment : writeTimeVariables) {
      builder.add(toCQLWord(fragment));
    }
    return builder.build();
  }

  private static CQLWord toCQLWord(CQLFragment fragment) {
    // transform all CQL fragments into CQL identifiers since that's the way they will be searched
    // for in DefaultMapping
    if (fragment instanceof CQLWord) {
      return ((CQLWord) fragment);
    }
    String internal = fragment.render(INTERNAL);
    return CQLWord.fromInternal(internal);
  }

  private static boolean containsConstantExpressions(Collection<?> coll) {
    return coll.stream().anyMatch(TypedCQLLiteral.class::isInstance);
  }

  private static boolean containsFunctionCalls(Collection<?> coll) {
    return containsFunctionCalls(coll, Predicates.alwaysTrue());
  }

  private static boolean containsFunctionCalls(
      Collection<?> coll, Predicate<? super FunctionCall> predicate) {
    return coll.stream()
        .filter(FunctionCall.class::isInstance)
        .map(FunctionCall.class::cast)
        .anyMatch(predicate);
  }

  private static boolean containsWritetimeOrTTLFunctionCalls(
      Multimap<MappingField, CQLFragment> mappings) {
    return mappings.entries().stream()
        .anyMatch(
            entry ->
                entry.getKey() instanceof FunctionCall && entry.getValue() instanceof FunctionCall);
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

  private static boolean checkLiteralSelectorsSupported(CqlSession session) {
    // Literal selectors are supported starting with C* 3.11.5
    // https://issues.apache.org/jira/browse/CASSANDRA-9243
    try {
      session.execute("SELECT (int)1 FROM system.local");
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
