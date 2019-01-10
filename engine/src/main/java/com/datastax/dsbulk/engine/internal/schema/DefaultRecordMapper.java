/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.function.BiFunction;
import org.jetbrains.annotations.Nullable;

public class DefaultRecordMapper implements RecordMapper {

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
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (Field field : record.fields()) {
        Collection<CQLFragment> variables = mapping.fieldToVariables(field);
        // Note: when loading, a field can be mapped to one or more variables.
        if (!variables.isEmpty()) {
          for (CQLFragment variable : variables) {
            String name = variable.render(VARIABLE);
            DataType cqlType = insertStatement.getVariables().getType(name);
            TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
            if (fieldType != null) {
              Object raw = record.getFieldValue(field);
              bindColumn(bs, variable, name, raw, cqlType, fieldType);
            }
          }
        } else if (!allowExtraFields) {
          // the field wasn't mapped to any known variable
          throw InvalidMappingException.extraneousField(field);
        }
      }
      ensurePrimaryKeySet(bs);
      if (protocolVersion.compareTo(ProtocolVersion.V4) < 0) {
        ensureAllVariablesSet(bs);
      }
      record.clear();
      return bs;
    } catch (Exception e) {
      return new UnmappableStatement(record, e);
    }
  }

  private <T> void bindColumn(
      BoundStatement bs,
      CQLFragment variable,
      String name,
      @Nullable T raw,
      DataType cqlType,
      TypeToken<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.serialize(raw, protocolVersion);
    if (isNull(bb, cqlType)) {
      if (variable instanceof CQLIdentifier && primaryKeyVariables.contains(variable)) {
        throw InvalidMappingException.nullPrimaryKey((CQLIdentifier) variable);
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

  private void ensureAllFieldsPresent(Set<Field> recordFields) {
    ColumnDefinitions variables = insertStatement.getVariables();
    for (int i = 0; i < variables.size(); i++) {
      CQLIdentifier variable = CQLIdentifier.fromInternal(variables.getName(i));
      Collection<Field> fields = mapping.variableToFields(variable);
      // Note: in practice, there can be only one field mapped to a given variable when loading
      for (Field field : fields) {
        if (!recordFields.contains(field)) {
          throw InvalidMappingException.missingField(field, variable);
        }
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatement bs) {
    for (CQLIdentifier variable : primaryKeyVariables) {
      if (!bs.isSet(variable.render(VARIABLE))) {
        throw InvalidMappingException.unsetPrimaryKey(variable);
      }
    }
  }

  private void ensureAllVariablesSet(BoundStatement bs) {
    ColumnDefinitions variables = insertStatement.getVariables();
    for (int i = 0; i < variables.size(); i++) {
      if (!bs.isSet(i)) {
        bs.setToNull(i);
      }
    }
  }
}
