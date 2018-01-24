/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
