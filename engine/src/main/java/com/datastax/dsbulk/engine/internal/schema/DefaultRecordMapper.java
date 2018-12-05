/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BiFunction;

public class DefaultRecordMapper implements RecordMapper {

  private static final String FIELD = "field";
  private static final String CQL_TYPE = "cqlType";

  private final PreparedStatement insertStatement;
  private final int[] pkIndices;
  private final int[] ccIndices;
  private final ProtocolVersion protocolVersion;
  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  private final BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      QueryInspector queryInspector,
      TableMetadata table) {
    this(
        insertStatement,
        DriverCoreHooks.partitionKeyIndices(insertStatement.getPreparedId()),
        findCCIndices(queryInspector, table),
        DriverCoreHooks.protocolVersion(insertStatement.getPreparedId()),
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  @VisibleForTesting
  DefaultRecordMapper(
      PreparedStatement insertStatement,
      int[] pkIndices,
      int[] ccIndices,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.pkIndices = pkIndices;
    this.ccIndices = ccIndices;
    this.protocolVersion = protocolVersion;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullToUnset = nullToUnset;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
    this.boundStatementFactory = boundStatementFactory;
  }

  @Override
  public Statement map(Record record) {
    String currentField = null;
    String variable = null;
    Object raw = null;
    DataType cqlType = null;
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (String field : record.fields()) {
        currentField = field;
        variable = mapping.fieldToVariable(field);
        if (variable != null) {
          cqlType = insertStatement.getVariables().getType(variable);
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          if (fieldType != null) {
            raw = record.getFieldValue(field);
            bindColumn(bs, variable, raw, cqlType, fieldType);
          }
        } else if (!allowExtraFields) {
          throw new InvalidMappingException(
              "Extraneous field "
                  + field
                  + " was found in record. "
                  + "Please declare it explicitly in the mapping "
                  + "or set schema.allowExtraFields to true.");
        }
      }
      ensurePrimaryKeySet(bs);
      ensureClusteringKeySet(bs);
      record.clear();
      return bs;
    } catch (Exception e) {
      String finalCurrentField = currentField;
      String finalVariable = variable;
      Object finalRaw = raw;
      DataType finalCqlType = cqlType;
      return new UnmappableStatement(
          record,
          Suppliers.memoize(
              () ->
                  URIUtils.addParamsToURI(
                      record.getLocation(),
                      FIELD,
                      finalCurrentField,
                      finalVariable,
                      finalRaw == null ? "" : finalRaw.toString(),
                      CQL_TYPE,
                      finalCqlType == null ? "" : finalCqlType.toString())),
          e);
    }
  }

  private <T> void bindColumn(
      BoundStatement bs,
      String variable,
      T raw,
      DataType cqlType,
      TypeToken<? extends T> javaType) {
    // the mapping provides unquoted variable names,
    // so we need to quote them now
    String name = Metadata.quoteIfNecessary(variable);
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.serialize(raw, protocolVersion);
    // Account for nullToUnset.
    if (isNull(bb, cqlType)) {
      if (isPrimaryKey(variable)) {
        throw new InvalidMappingException(
            "Primary key column "
                + Metadata.quoteIfNecessary(variable)
                + " cannot be mapped to null. "
                + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
      } else if (isClusteringKey(variable)) {
        throw new InvalidMappingException(
            "Clustering key column "
                + Metadata.quoteIfNecessary(variable)
                + " cannot be mapped to null. "
                + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
      }
      if (nullToUnset) {
        return;
      }
    }
    bs.setBytesUnsafe(name, bb);
  }

  private boolean isNull(ByteBuffer bb, DataType cqlType) {
    if (bb == null) {
      return true;
    }
    if (bb.hasRemaining()) {
      return false;
    }
    switch (cqlType.getName()) {
      case TEXT:
      case VARCHAR:
      case ASCII:
        // empty strings are encoded as zero-length buffers,
        // and should not be considered as nulls.
        return false;
      default:
        return true;
    }
  }

  private boolean isPrimaryKey(String variable) {
    return Arrays.binarySearch(pkIndices, insertStatement.getVariables().getIndexOf(variable)) >= 0;
  }

  private boolean isClusteringKey(String variable) {
    return Arrays.binarySearch(ccIndices, insertStatement.getVariables().getIndexOf(variable)) >= 0;
  }

  private void ensureAllFieldsPresent(Set<String> recordFields) {
    ColumnDefinitions variables = insertStatement.getVariables();
    for (int i = 0; i < variables.size(); i++) {
      String variable = variables.getName(i);
      String field = mapping.variableToField(variable);
      if (!recordFields.contains(field)) {
        throw new InvalidMappingException(
            "Required field "
                + field
                + " (mapped to column "
                + Metadata.quoteIfNecessary(variable)
                + ") was missing from record. "
                + "Please remove it from the mapping "
                + "or set schema.allowMissingFields to true.");
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatement bs) {
    if (pkIndices != null) {
      for (int pkIndex : pkIndices) {
        if (!bs.isSet(pkIndex)) {
          String variable = insertStatement.getVariables().getName(pkIndex);
          throw new InvalidMappingException(
              "Primary key column "
                  + Metadata.quoteIfNecessary(variable)
                  + " cannot be left unmapped. "
                  + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
        }
      }
    }
  }

  private void ensureClusteringKeySet(BoundStatement bs) {
    if (ccIndices.length > 0) {
      for (int ccIndex : ccIndices) {
        if (!bs.isSet(ccIndex)) {
          String variable = insertStatement.getVariables().getName(ccIndex);
          throw new InvalidMappingException(
              "Clustering key column "
                  + Metadata.quoteIfNecessary(variable)
                  + " cannot be left unmapped. "
                  + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
        }
      }
    }
  }

  private static int[] findCCIndices(QueryInspector query, TableMetadata metadata) {
    int i = 0;
    int[] indicies = new int[metadata.getClusteringColumns().size()];
    for (ColumnMetadata clusterColumnName : metadata.getClusteringColumns()) {
      indicies[i] =
          query.getBoundVariables().keySet().asList().indexOf(clusterColumnName.getName());
      i++;
    }
    return indicies;
  }
}
