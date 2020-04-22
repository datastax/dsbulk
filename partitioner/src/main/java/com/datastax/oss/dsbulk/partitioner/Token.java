/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A token in a ring.
 *
 * @param <T> The token value type.
 */
public interface Token<T extends Number> extends Comparable<Token<T>> {

  /** @return This token's value. */
  @NonNull
  T value();
}
