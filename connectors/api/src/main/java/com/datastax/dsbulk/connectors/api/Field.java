/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A field in a record. Fields can be {@linkplain IndexedField indexed} or {@linkplain MappedField
 * mapped}.
 */
public interface Field {

  /**
   * @return a generic description of the field, mainly for error reporting purposes; usually its
   *     name or index.
   */
  @NonNull
  String getFieldDescription();
}
