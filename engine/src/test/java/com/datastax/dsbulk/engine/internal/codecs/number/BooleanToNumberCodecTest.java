/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.junit.jupiter.api.Test;

class BooleanToNumberCodecTest {

  @Test
  void should_convert_from_valid_external() {

    assertThat(new BooleanToNumberCodec<>(TypeCodecs.TINYINT, newArrayList(ONE, ONE.negate())))
        .convertsFromExternal(true)
        .toInternal((byte) 1)
        .convertsFromExternal(false)
        .toInternal((byte) -1)
        .convertsFromInternal((byte) 1)
        .toExternal(true)
        .convertsFromInternal((byte) -1)
        .toExternal(false)
        .cannotConvertFromInternal((byte) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
