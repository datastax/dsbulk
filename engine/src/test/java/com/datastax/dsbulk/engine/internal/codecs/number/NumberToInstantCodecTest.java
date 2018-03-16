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
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class NumberToInstantCodecTest {

  private NumberToInstantCodec<Long> codec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    assertThat(codec)
        .convertsFromExternal(123456L)
        .toInternal(Instant.ofEpochMilli(123456L))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(codec)
        .convertsFromInternal(Instant.ofEpochMilli(123456L))
        .toExternal(123456L)
        .convertsFromInternal(null)
        .toExternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
