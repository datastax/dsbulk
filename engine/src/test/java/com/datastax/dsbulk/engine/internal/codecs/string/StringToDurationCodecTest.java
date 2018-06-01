/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.core.Duration;
import org.junit.jupiter.api.Test;

class StringToDurationCodecTest {

  private final long nanosPerMinute = 60 * 1000L * 1000L * 1000L;

  private final Duration duration = Duration.newInstance(15, 0, 130 * nanosPerMinute);

  private final StringToDurationCodec codec = new StringToDurationCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("1y3mo2h10m") // standard pattern
        .toInternal(duration)
        .convertsFromExternal("P1Y3MT2H10M") // ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal("P0001-03-00T02:10:00") // alternative ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(duration)
        .toExternal("1y3mo2h10m")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("1Y3M4D") // The minutes should be after days
        .cannotConvertFromExternal("not a valid duration");
  }
}
