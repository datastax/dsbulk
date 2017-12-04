/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.json.internal;

import com.datastax.driver.core.DataType;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

public class SchemaFreeJsonRecordMetadata implements RecordMetadata {

  private static final TypeToken<JsonNode> JSON_NODE_TYPE_TOKEN = TypeToken.of(JsonNode.class);

  @Override
  public TypeToken<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    return JSON_NODE_TYPE_TOKEN;
  }
}
