/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import com.datastax.driver.core.DataType;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

/**
 * Defines metadata applicable to a {@link com.datastax.dsbulk.connectors.api.Record record}, in
 * particular which field types it contains.
 */
public interface RecordMetadata {

  /**
   * Returns the type of the given field.
   *
   * @param field the field name.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field, or {@code null} if the field isn't defined in this schema.
   */
  TypeToken<?> getFieldType(@NotNull String field, @NotNull DataType cqlType);
}
