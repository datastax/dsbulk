/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import org.jetbrains.annotations.NotNull;

/** A field in a {@link Record} identified by an alphanumeric name. */
public interface MappedField extends Field {

  /** @return The field name. */
  @NotNull
  String getFieldName();
}
