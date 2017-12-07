/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;

import org.junit.jupiter.api.Test;

/** */
class NumberToBooleanCodecTest {

  @Test
  void should_convert_to_valid_input() {

    assertThat(new NumberToBooleanCodec<>(Byte.class, newArrayList(ONE, ONE.negate())))
        .convertsFrom((byte) 1)
        .to(true)
        .convertsFrom((byte) -1)
        .to(false)
        .convertsTo(true)
        .from((byte) 1)
        .convertsTo(false)
        .from((byte) -1)
        .cannotConvertFrom((byte) 0)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }
}
