/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

/** A field in a {@link Record} identified by a zero-based integer index. */
public interface IndexedField extends Field {

  /** @return The field zero-based index. */
  int getFieldIndex();
}
