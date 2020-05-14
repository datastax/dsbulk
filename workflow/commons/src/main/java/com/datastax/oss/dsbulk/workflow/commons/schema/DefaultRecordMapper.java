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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.InvalidMappingException;
import com.datastax.oss.dsbulk.mapping.Mapping;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DefaultRecordMapper implements RecordMapper {

  private final PreparedStatement insertStatement;
  private final ImmutableSet<CQLWord> partitionKeyVariables;
  private final ImmutableSet<CQLWord> clusteringColumnVariables;
  private final ProtocolVersion protocolVersion;
  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean nullToUnset;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;
  private final Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory;
  private final ImmutableMap<CQLWord, List<Integer>> variablesToIndices;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Set<CQLWord> partitionKeyVariables,
      Set<CQLWord> clusteringColumnVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    this(
        insertStatement,
        partitionKeyVariables,
        clusteringColumnVariables,
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
      Set<CQLWord> partitionKeyVariables,
      Set<CQLWord> clusteringColumnVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory) {
    this.insertStatement = insertStatement;
    this.partitionKeyVariables = ImmutableSet.copyOf(partitionKeyVariables);
    this.clusteringColumnVariables = ImmutableSet.copyOf(clusteringColumnVariables);
    this.protocolVersion = protocolVersion;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullToUnset = nullToUnset;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
    this.boundStatementBuilderFactory = boundStatementBuilderFactory;
    this.variablesToIndices = buildVariablesToIndices();
  }

  @NonNull
  @Override
  public BatchableStatement<?> map(@NonNull Record record) {
    try {
      if (!allowMissingFields) {
        ensureAllFieldsPresent(record.fields());
      }
      BoundStatementBuilder builder = boundStatementBuilderFactory.apply(insertStatement);
      ColumnDefinitions variableDefinitions = insertStatement.getVariableDefinitions();
      for (Field field : record.fields()) {
        Set<CQLWord> variables = mapping.fieldToVariables(field);
        if (!variables.isEmpty()) {
          for (CQLWord variable : variables) {
            CqlIdentifier name = variable.asIdentifier();
            DataType cqlType = variableDefinitions.get(name).getType();
            GenericType<?> fieldType = recordMetadata.getFieldType(field, cqlType);
            Object raw = record.getFieldValue(field);
            builder = bindColumn(builder, variable, raw, cqlType, fieldType);
          }
        } else if (!allowExtraFields) {
          // the field wasn't mapped to any known variable
          throw InvalidMappingException.extraneousField(field);
        }
      }
      ensurePrimaryKeySet(builder);
      if (protocolVersion.getCode() < DefaultProtocolVersion.V4.getCode()) {
        ensureAllVariablesSet(builder);
      }
      record.clear();
      BoundStatement bs = builder.build();
      return new BulkBoundStatement<>(record, bs);
    } catch (Exception e) {
      return new UnmappableStatement(record, e);
    }
  }

  private <T> BoundStatementBuilder bindColumn(
      BoundStatementBuilder builder,
      CQLWord variable,
      @Nullable T raw,
      DataType cqlType,
      GenericType<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.encode(raw, builder.protocolVersion());
    boolean isNull = isNull(bb, cqlType);
    if (isNull || isEmpty(bb)) {
      if (partitionKeyVariables.contains(variable)) {
        throw isNull
            ? InvalidMappingException.nullPrimaryKey(variable)
            : InvalidMappingException.emptyPrimaryKey(variable);
      }
    }
    if (isNull) {
      if (clusteringColumnVariables.contains(variable)) {
        throw InvalidMappingException.nullPrimaryKey(variable);
      }
      if (nullToUnset) {
        return builder;
      }
    }
    for (int index : variablesToIndices.get(variable)) {
      builder = builder.setBytesUnsafe(index, bb);
    }
    return builder;
  }

  private boolean isNull(ByteBuffer bb, DataType cqlType) {
    if (bb == null) {
      return true;
    }
    switch (cqlType.getProtocolCode()) {
      case VARCHAR:
      case ASCII:
      case BLOB:
        // zero-length buffers should not be considered as nulls for
        // these CQL types.
        return false;
      default:
        return !bb.hasRemaining();
    }
  }

  private boolean isEmpty(ByteBuffer bb) {
    return bb == null || !bb.hasRemaining();
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
    for (CQLWord variable : partitionKeyVariables) {
      for (int index : variablesToIndices.get(variable)) {
        if (!bs.isSet(index)) {
          throw InvalidMappingException.unsetPrimaryKey(variable);
        }
      }
    }
    for (CQLWord variable : clusteringColumnVariables) {
      for (int index : variablesToIndices.get(variable)) {
        if (!bs.isSet(index)) {
          throw InvalidMappingException.unsetPrimaryKey(variable);
        }
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

  private ImmutableMap<CQLWord, List<Integer>> buildVariablesToIndices() {
    Map<CQLWord, List<Integer>> variablesToIndices = new HashMap<>();
    ColumnDefinitions variables = insertStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      CQLWord name = CQLWord.fromCqlIdentifier(variables.get(i).getName());
      List<Integer> indices = variablesToIndices.computeIfAbsent(name, k -> new ArrayList<>());
      indices.add(i);
    }
    return ImmutableMap.copyOf(variablesToIndices);
  }
}
