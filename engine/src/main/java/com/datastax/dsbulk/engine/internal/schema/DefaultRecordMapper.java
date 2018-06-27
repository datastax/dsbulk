/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;

import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public class DefaultRecordMapper implements RecordMapper {

  private static final String FIELD = "field";
  private static final String CQL_TYPE = "cqlType";

  private final PreparedStatement insertStatement;
  private final List<Integer> pkIndices;
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
      boolean allowMissingFields) {
    this(
        insertStatement,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields,
        (mappedRecord, statement) ->
            new BulkBoundStatement<>(mappedRecord, insertStatement.bind()));
  }

  @VisibleForTesting
  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.pkIndices = insertStatement.getPrimaryKeyIndices();
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
    CqlIdentifier variable = null;
    Object raw = null;
    DataType cqlType = null;
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      ColumnDefinitions variableDefinitions = insertStatement.getVariableDefinitions();
      for (String field : record.fields()) {
        currentField = field;
        variable = mapping.fieldToVariable(field);
        if (variable != null) {
          cqlType = variableDefinitions.get(variable).getType();
          GenericType<?> fieldType = recordMetadata.getFieldType(field, cqlType);
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
      record.clear();
      return bs;
    } catch (Exception e) {
      String finalCurrentField = currentField;
      CqlIdentifier finalVariable = variable;
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
                      finalVariable == null ? null : finalVariable.asInternal(),
                      finalRaw == null ? "" : finalRaw.toString(),
                      CQL_TYPE,
                      finalCqlType == null ? "" : finalCqlType.toString())),
          e);
    }
  }

  private <T> void bindColumn(
      BoundStatement bs,
      CqlIdentifier variable,
      T raw,
      DataType cqlType,
      GenericType<? extends T> javaType) {
    // the mapping provides unquoted variable names,
    // so we need to quote them now
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.encode(raw, bs.protocolVersion());
    // Account for nullToUnset.
    if (isNull(bb, cqlType)) {
      if (isPrimaryKey(variable)) {
        throw new InvalidMappingException(
            "Primary key column "
                + variable.asCql(true)
                + " cannot be mapped to null. "
                + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
      }
      if (nullToUnset) {
        return;
      }
    }
    bs.setBytesUnsafe(variable, bb);
  }

  private boolean isNull(ByteBuffer bb, DataType cqlType) {
    if (bb == null) {
      return true;
    }
    if (bb.hasRemaining()) {
      return false;
    }
    switch (cqlType.getProtocolCode()) {
      case VARCHAR:
      case ASCII:
        // empty strings are encoded as zero-length buffers,
        // and should not be considered as nulls.
        return false;
      default:
        return true;
    }
  }

  private boolean isPrimaryKey(CqlIdentifier variable) {
    return pkIndices.contains(insertStatement.getVariableDefinitions().firstIndexOf(variable));
  }

  private void ensureAllFieldsPresent(Set<String> recordFields) {
    ColumnDefinitions variables = insertStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      CqlIdentifier variable = variables.get(i).getName();
      String field = mapping.variableToField(variable);
      if (!recordFields.contains(field)) {
        throw new InvalidMappingException(
            "Required field "
                + field
                + " (mapped to column "
                + variable.asCql(true)
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
          CqlIdentifier variable = insertStatement.getVariableDefinitions().get(pkIndex).getName();
          throw new InvalidMappingException(
              "Primary key column "
                  + variable.asCql(true)
                  + " cannot be left unmapped. "
                  + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
        }
      }
    }
  }
}
