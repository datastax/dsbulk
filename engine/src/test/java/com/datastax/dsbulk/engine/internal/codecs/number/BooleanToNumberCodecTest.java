/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
