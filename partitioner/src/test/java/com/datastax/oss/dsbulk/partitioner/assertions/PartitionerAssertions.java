/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner.assertions;

import com.datastax.oss.dsbulk.partitioner.Token;
import com.datastax.oss.dsbulk.partitioner.TokenRange;
import com.datastax.oss.dsbulk.tests.assertions.TestAssertions;

public class PartitionerAssertions extends TestAssertions {

  public static <V extends Number, T extends Token<V>> TokenRangeAssert<V, T> assertThat(
      TokenRange<V, T> actual) {
    return new TokenRangeAssert<>(actual);
  }
}
