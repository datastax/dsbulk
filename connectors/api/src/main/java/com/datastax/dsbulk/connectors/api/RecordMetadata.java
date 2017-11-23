/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api;

import com.datastax.driver.core.DataType;
import com.datastax.dsbulk.connectors.api.internal.SchemaFreeRecordMetadata;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

/** Defines metadata applicable to a {@link com.datastax.dsbulk.connectors.api.Record record}. */
public interface RecordMetadata {

  /**
   * The default instance. This instance always assumes that fields exist, and that their types are
   * {@link String}.
   */
  RecordMetadata DEFAULT = new SchemaFreeRecordMetadata();

  /**
   * Returns the type of the given field.
   *
   * @param field the field name.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field, or {@code null} if the field isn't defined in this schema.
   */
  TypeToken<?> getFieldType(@NotNull String field, @NotNull DataType cqlType);
}
