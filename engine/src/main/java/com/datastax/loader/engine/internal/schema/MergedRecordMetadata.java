/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.DataType;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.connectors.api.internal.DefaultRecordMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public class MergedRecordMetadata extends DefaultRecordMetadata {

  private final RecordMetadata fallback;

  public MergedRecordMetadata(
      ImmutableMap<String, TypeToken<?>> fieldsToTypes, RecordMetadata fallback) {
    super(fieldsToTypes);
    this.fallback = fallback;
  }

  @Override
  public TypeToken<?> getFieldType(String field, DataType cqlType) {
    TypeToken<?> fieldType = super.getFieldType(field, cqlType);
    if (fieldType == null) {
      fieldType = fallback.getFieldType(field, cqlType);
    }
    return fieldType;
  }
}
