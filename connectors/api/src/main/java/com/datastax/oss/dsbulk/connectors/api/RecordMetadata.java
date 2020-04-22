/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.connectors.api;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Defines metadata applicable to a {@link Record record}, in particular which field types it
 * contains.
 */
public interface RecordMetadata {

  /**
   * Returns the type of the given field.
   *
   * @param field the field to get the type from.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field.
   */
  @NonNull
  GenericType<?> getFieldType(@NonNull Field field, @NonNull DataType cqlType);
}
