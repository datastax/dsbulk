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
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

public class SchemaFreeRecordMetadata implements RecordMetadata {

  private static final TypeToken<String> STRING_TYPE_TOKEN = TypeToken.of(String.class);

  @Override
  public TypeToken<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    return STRING_TYPE_TOKEN;
  }
}
