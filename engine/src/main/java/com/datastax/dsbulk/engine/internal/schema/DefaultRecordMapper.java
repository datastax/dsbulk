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

import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

public class DefaultRecordMapper implements RecordMapper {

  private final PreparedStatement insertStatement;
  private final ImmutableSet<CQLWord> primaryKeyVariables;
  private final ProtocolVersion protocolVersion;
  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean nullToUnset;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;
  private final Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Set<CQLWord> primaryKeyVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    this(
        insertStatement,
        primaryKeyVariables,
        protocolVersion,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields,
        ps -> ps.boundStatementBuilder());
  }

  @VisibleForTesting
  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Set<CQLWord> primaryKeyVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory) {
    this.insertStatement = insertStatement;
    this.primaryKeyVariables = ImmutableSet.copyOf(primaryKeyVariables);
    this.protocolVersion = protocolVersion;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullToUnset = nullToUnset;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
    this.boundStatementBuilderFactory = boundStatementBuilderFactory;
  }

  @NonNull
  @Override
  public BatchableStatement<?> map(@NonNull Record record) {
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatementBuilder bs = boundStatementBuilderFactory.apply(insertStatement);
      ColumnDefinitions variableDefinitions = insertStatement.getVariableDefinitions();
      for (Field field : record.fields()) {
        Set<CQLWord> variables = mapping.fieldToVariables(field);
        if (!variables.isEmpty()) {
          for (CQLWord variable : variables) {
            CqlIdentifier name = variable.asIdentifier();
            DataType cqlType = variableDefinitions.get(name).getType();
            GenericType<?> fieldType = recordMetadata.getFieldType(field, cqlType);
            Object raw = record.getFieldValue(field);
            bs = bindColumn(bs, variable, raw, cqlType, fieldType);
          }
        } else if (!allowExtraFields) {
          // the field wasn't mapped to any known variable
          throw InvalidMappingException.extraneousField(field);
        }
      }
      ensurePrimaryKeySet(bs);
      if (protocolVersion.getCode() < DefaultProtocolVersion.V4.getCode()) {
        ensureAllVariablesSet(bs);
      }
      record.clear();
      return new BulkBoundStatement<>(record, bs.build());
    } catch (Exception e) {
      return new UnmappableStatement(record, e);
    }
  }

  private <T> BoundStatementBuilder bindColumn(
      BoundStatementBuilder bs,
      CQLWord variable,
      @Nullable T raw,
      DataType cqlType,
      GenericType<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.encode(raw, bs.protocolVersion());
    if (isNull(bb, cqlType)) {
      if (primaryKeyVariables.contains(variable)) {
        throw InvalidMappingException.nullPrimaryKey(variable);
      }
      if (nullToUnset) {
        return bs;
      }
    }
    return bs.setBytesUnsafe(variable.asIdentifier(), bb);
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

  private void ensureAllFieldsPresent(Set<Field> recordFields) {
    ColumnDefinitions variables = insertStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      CQLWord variable = CQLWord.fromCqlIdentifier(variables.get(i).getName());
      Collection<Field> fields = mapping.variableToFields(variable);
      // Note: in practice, there can be only one field mapped to a given variable when loading
      for (Field field : fields) {
        if (!recordFields.contains(field)) {
          throw InvalidMappingException.missingField(field, variable);
        }
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatementBuilder bs) {
    for (CQLWord variable : primaryKeyVariables) {
      if (!bs.isSet(variable.asIdentifier())) {
        throw InvalidMappingException.unsetPrimaryKey(variable);
      }
    }
  }

  private void ensureAllVariablesSet(BoundStatementBuilder bs) {
    ColumnDefinitions variables = insertStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      if (!bs.isSet(i)) {
        bs = bs.setToNull(i);
      }
    }
  }
}
