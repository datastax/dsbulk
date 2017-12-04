/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.driver.core.DataType;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

public class DefaultRecordMetadata implements RecordMetadata {

  private final ImmutableMap<String, TypeToken<?>> fieldsToTypes;

  public DefaultRecordMetadata(ImmutableMap<String, TypeToken<?>> fieldsToTypes) {
    this.fieldsToTypes = fieldsToTypes;
  }

  @Override
  public TypeToken<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    return fieldsToTypes.get(field);
  }
}
