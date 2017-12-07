/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.driver.core.TypeCodec.tinyInt;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;

import org.junit.jupiter.api.Test;

/** */
class BooleanToNumberCodecTest {

  @Test
  void should_convert_from_valid_input() {

    assertThat(new BooleanToNumberCodec<>(tinyInt(), newArrayList(ONE, ONE.negate())))
        .convertsFrom(true)
        .to((byte) 1)
        .convertsFrom(false)
        .to((byte) -1)
        .convertsTo((byte) 1)
        .from(true)
        .convertsTo((byte) -1)
        .from(false)
        .cannotConvertTo((byte) 0)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }
}
