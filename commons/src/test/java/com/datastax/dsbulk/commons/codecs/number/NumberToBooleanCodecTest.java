/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;

import org.junit.jupiter.api.Test;

class NumberToBooleanCodecTest {

  @Test
  void should_convert_from_valid_internal() {

    assertThat(new NumberToBooleanCodec<>(Byte.class, newArrayList(ONE, ONE.negate())))
        .convertsFromExternal((byte) 1)
        .toInternal(true)
        .convertsFromExternal((byte) -1)
        .toInternal(false)
        .convertsFromInternal(true)
        .toExternal((byte) 1)
        .convertsFromInternal(false)
        .toExternal((byte) -1)
        .cannotConvertFromExternal((byte) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
