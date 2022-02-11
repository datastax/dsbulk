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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.Mapping;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import reactor.core.publisher.Flux;

public class DefaultRecordMapper implements RecordMapper {

  private final List<PreparedStatement> insertStatements;
  private final ImmutableSet<CQLWord> partitionKeyVariables;
  private final ImmutableSet<CQLWord> clusteringColumnVariables;
  private final ImmutableSet<CQLWord> primaryKeyVariables;
  private final ProtocolVersion protocolVersion;
  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final boolean nullToUnset;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;
  private final Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory;
  private final int size;

  public DefaultRecordMapper(
      List<PreparedStatement> insertStatements,
      Set<CQLWord> partitionKeyVariables,
      Set<CQLWord> clusteringColumnVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    this(
        insertStatements,
        partitionKeyVariables,
        clusteringColumnVariables,
        protocolVersion,
        mapping,
        recordMetadata,
        nullToUnset,
        allowExtraFields,
        allowMissingFields,
        PreparedStatement::boundStatementBuilder);
  }

  @VisibleForTesting
  DefaultRecordMapper(
      List<PreparedStatement> insertStatements,
      Set<CQLWord> partitionKeyVariables,
      Set<CQLWord> clusteringColumnVariables,
      ProtocolVersion protocolVersion,
      Mapping mapping,
      RecordMetadata recordMetadata,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      Function<PreparedStatement, BoundStatementBuilder> boundStatementBuilderFactory) {
    this.insertStatements = ImmutableList.copyOf(insertStatements);
    this.partitionKeyVariables = ImmutableSet.copyOf(partitionKeyVariables);
    this.clusteringColumnVariables = ImmutableSet.copyOf(clusteringColumnVariables);
    this.protocolVersion = protocolVersion;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullToUnset = nullToUnset;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
    this.boundStatementBuilderFactory = boundStatementBuilderFactory;
    primaryKeyVariables =
        ImmutableSet.<CQLWord>builder()
            .addAll(partitionKeyVariables)
            .addAll(clusteringColumnVariables)
            .build();
    size = insertStatements.size();
  }

  @NonNull
  @Override
  public Flux<BatchableStatement<?>> map(@NonNull Record record) {
    try {
      Set<Field> recordFields = record.fields();
      if (!allowMissingFields) {
        ensureAllFieldsPresent(recordFields);
      }
      if (!allowExtraFields) {
        ensureNoExtraFields(recordFields);
      }
      if (size == 1) {
        return Flux.just(bindStatement(record, insertStatements.get(0)));
      } else {
        BatchableStatement<?>[] statements = new BatchableStatement<?>[size];
        for (int i = 0; i < size; i++) {
          statements[i] = bindStatement(record, insertStatements.get(i));
        }
        // Note: we only emit the generated bound statements if all of them were successfully
        // created; if any fails, we return one single UnmappableStatement instead.
        return Flux.fromArray(statements);
      }
    } catch (Exception e) {
      // We don't emit errors here, instead we wrap record+error in a special type that looks like a
      // normal item being emitted but is going to be filtered later on by downstream consumers.
      return Flux.just(new UnmappableStatement(record, e));
    } finally {
      // To save memory, we delete the record's original data now since we won't need it anymore.
      record.clear();
    }
  }

  private MappedBoundStatement bindStatement(Record record, PreparedStatement insertStatement) {
    BoundStatementBuilder builder = boundStatementBuilderFactory.apply(insertStatement);
    ColumnDefinitions variableDefinitions = insertStatement.getVariableDefinitions();
    for (Field field : record.fields()) {
      Set<CQLWord> variables = mapping.fieldToVariables(field);
      for (CQLWord variable : variables) {
        CqlIdentifier name = variable.asIdentifier();
        if (size == 1 || variableDefinitions.contains(name)) {
          DataType cqlType = variableDefinitions.get(name).getType();
          GenericType<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          Object raw = record.getFieldValue(field);
          builder = bindColumn(builder, field, variable, raw, cqlType, fieldType);
        }
      }
    }
    ensurePrimaryKeySet(builder);
    if (protocolVersion.getCode() < DefaultProtocolVersion.V4.getCode()) {
      ensureAllVariablesSet(builder, insertStatement);
    }
    BoundStatement bs = builder.build();
    return new MappedBoundStatement(record, bs);
  }

  private <T> BoundStatementBuilder bindColumn(
      BoundStatementBuilder builder,
      Field field,
      CQLWord variable,
      @Nullable T raw,
      DataType cqlType,
      GenericType<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb;
    try {
      bb = codec.encode(raw, builder.protocolVersion());
    } catch (Exception e) {
      throw InvalidMappingException.encodeFailed(field, variable, javaType, cqlType, raw, e);
    }
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
    return builder.setBytesUnsafe(variable.asIdentifier(), bb);
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
    for (Field field : mapping.fields()) {
      if (!recordFields.contains(field)) {
        Set<CQLWord> variables = mapping.fieldToVariables(field);
        throw InvalidMappingException.missingField(field, variables);
      }
    }
  }

  private void ensureNoExtraFields(Set<Field> recordFields) {
    for (Field field : recordFields) {
      if (!mapping.fields().contains(field)) {
        throw InvalidMappingException.extraneousField(field);
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatementBuilder bs) {
    for (CQLWord variable : primaryKeyVariables) {
      for (int index : bs.allIndicesOf(variable.asIdentifier())) {
        if (!bs.isSet(index)) {
          throw InvalidMappingException.unsetPrimaryKey(variable);
        }
      }
    }
  }

  private void ensureAllVariablesSet(BoundStatementBuilder bs, PreparedStatement insertStatement) {
    ColumnDefinitions variables = insertStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      if (!bs.isSet(i)) {
        bs = bs.setToNull(i);
      }
    }
  }
}
