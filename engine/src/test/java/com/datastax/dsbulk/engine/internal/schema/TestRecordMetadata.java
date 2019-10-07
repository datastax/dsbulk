/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TestRecordMetadata implements RecordMetadata {

  private final ImmutableMap<Field, GenericType<?>> fieldsToTypes;

  TestRecordMetadata(ImmutableMap<Field, GenericType<?>> fieldsToTypes) {
    this.fieldsToTypes = fieldsToTypes;
  }

  @NonNull
  @Override
  public GenericType<?> getFieldType(@NonNull Field field, @NonNull DataType cqlType) {
    return fieldsToTypes.get(field);
  }
}
