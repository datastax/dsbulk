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
        .convertsFrom(123456L)
        .to(Instant.ofEpochMilli(123456L))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(codec)
        .convertsTo(Instant.ofEpochMilli(123456L))
        .from(123456L)
        .convertsTo(null)
        .from(null)
        .convertsTo(null)
        .from(null);
  }
}
