/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class NumberToInstantCodecTest {

  private NumberToInstantCodec<Long> codec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH);

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
