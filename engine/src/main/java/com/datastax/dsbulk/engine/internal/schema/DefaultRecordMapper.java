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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiFunction;

public class DefaultRecordMapper implements RecordMapper {

  private static final String FIELD = "field";
  private static final String CQL_TYPE = "cqlType";

  private final PreparedStatement insertStatement;
  private final ImmutableSet<CQLIdentifier> primaryKeyVariables;
  private final ProtocolVersion protocolVersion;
  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean nullToUnset;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;
  private final BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Set<CQLIdentifier> primaryKeyVariables,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    this(
        insertStatement,
        primaryKeyVariables,
        DriverCoreHooks.protocolVersion(insertStatement.getPreparedId()),
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields,
        BulkBoundStatement::new);
  }

  @VisibleForTesting
  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Set<CQLIdentifier> primaryKeyVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.primaryKeyVariables = ImmutableSet.copyOf(primaryKeyVariables);
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
    Field field = null;
    CQLFragment variable = null;
    Object raw = null;
    DataType cqlType = null;
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (Field f : record.fields()) {
        field = f;
        variable = mapping.fieldToVariable(field);
        if (variable != null) {
          cqlType = insertStatement.getVariables().getType(variable.asVariable());
          TypeToken<?> fieldType = recordMetadata.getFieldType(f, cqlType);
          if (fieldType != null) {
            raw = record.getFieldValue(f);
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
      Field finalField = field;
      CQLFragment finalVariable = variable;
      Object finalRaw = raw;
      DataType finalCqlType = cqlType;
      return new UnmappableStatement(
          record,
          Suppliers.memoize(
              () ->
                  URIUtils.addParamsToURI(
                      record.getLocation(),
                      FIELD,
                      finalField == null ? "" : finalField.getFieldDescription(),
                      finalVariable == null ? "" : finalVariable.asInternal(),
                      finalRaw == null ? "" : finalRaw.toString(),
                      CQL_TYPE,
                      finalCqlType == null ? "" : finalCqlType.toString())),
          e);
    }
  }

  private <T> void bindColumn(
      BoundStatement bs,
      CQLFragment variable,
      T raw,
      DataType cqlType,
      TypeToken<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.serialize(raw, protocolVersion);
    if (isNull(bb, cqlType)) {
      if (primaryKeyVariables.contains(variable)) {
        throw new InvalidMappingException(
            "Primary key column "
                + variable.asCql()
                + " cannot be mapped to null. "
                + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
      }
      if (nullToUnset) {
        return;
      }
    }
    bs.setBytesUnsafe(variable.asVariable(), bb);
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

  private void ensureAllFieldsPresent(Set<Field> recordFields) {
    ColumnDefinitions variables = insertStatement.getVariables();
    for (int i = 0; i < variables.size(); i++) {
      CQLIdentifier variable = CQLIdentifier.fromInternal(variables.getName(i));
      Field field = mapping.variableToField(variable);
      if (!recordFields.contains(field)) {
        throw new InvalidMappingException(
            "Required field "
                + field
                + " (mapped to column "
                + variable.asCql()
                + ") was missing from record. "
                + "Please remove it from the mapping "
                + "or set schema.allowMissingFields to true.");
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatement bs) {
    for (CQLIdentifier variable : primaryKeyVariables) {
      if (!bs.isSet(variable.asVariable())) {
        throw new InvalidMappingException(
            "Primary key column "
                + variable.asCql()
                + " cannot be left unmapped. "
                + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
      }
    }
  }
}
