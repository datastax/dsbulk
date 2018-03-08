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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
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
import java.util.function.BiFunction;

public class DefaultRecordMapper implements RecordMapper {

  private static final String FIELD = "field";
  private static final String CQL_TYPE = "cqlType";

  private final PreparedStatement insertStatement;
  private int[] pkIndices;

  private final ProtocolVersion protocolVersion;

  private final Mapping mapping;

  private final RecordMetadata recordMetadata;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  private final BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset) {
    this(
        insertStatement,
        DriverCoreHooks.primaryKeyIndices(insertStatement.getPreparedId()),
        DriverCoreHooks.protocolVersion(insertStatement.getPreparedId()),
        mapping,
        recordMetadata,
        nullToUnset,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  @VisibleForTesting
  DefaultRecordMapper(
      PreparedStatement insertStatement,
      int[] pkIndices,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.pkIndices = pkIndices;
    this.protocolVersion = protocolVersion;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullToUnset = nullToUnset;
    this.boundStatementFactory = boundStatementFactory;
  }

  @Override
  public Statement map(Record record) {
    String currentField = null;
    String variable = null;
    Object raw = null;
    DataType cqlType = null;
    try {
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
        }
      }
      ensurePrimaryKeySet(bs);
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
                      finalRaw == null ? null : finalRaw.toString(),
                      CQL_TYPE,
                      finalCqlType == null ? null : finalCqlType.toString())),
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
    if (isNull(bb)) {
      if (isPrimaryKey(variable)) {
        throw new IllegalStateException(
            "Primary key column "
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

  private boolean isNull(ByteBuffer bb) {
    return bb == null || !bb.hasRemaining();
  }

  private boolean isPrimaryKey(String variable) {
    return Arrays.binarySearch(pkIndices, insertStatement.getVariables().getIndexOf(variable)) >= 0;
  }

  private void ensurePrimaryKeySet(BoundStatement bs) {
    for (int pkIndex : pkIndices) {
      if (!bs.isSet(pkIndex)) {
        String variable = insertStatement.getVariables().getName(pkIndex);
        throw new IllegalStateException(
            String.format(
                "Primary key column "
                    + Metadata.quoteIfNecessary(variable)
                    + " cannot be left unmapped. "
                    + "Check that your settings (schema.mapping or schema.query) match your dataset contents.",
                variable));
      }
    }
  }
}
