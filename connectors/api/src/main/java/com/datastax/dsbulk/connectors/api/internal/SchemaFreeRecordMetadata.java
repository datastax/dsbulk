/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.driver.core.DataType;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.google.common.reflect.TypeToken;

public class SchemaFreeRecordMetadata implements RecordMetadata {

  private static final TypeToken<String> STRING_TYPE_TOKEN = TypeToken.of(String.class);

  @Override
  public TypeToken<?> getFieldType(String field, DataType cqlType) {
    return STRING_TYPE_TOKEN;
  }
}
