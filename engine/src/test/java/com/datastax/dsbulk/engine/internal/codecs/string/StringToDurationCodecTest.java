/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.Duration;
import org.junit.Test;

public class StringToDurationCodecTest {

  static final long NANOS_PER_MINUTE = 60 * 1000L * 1000L * 1000L;

  Duration duration = Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE);

  StringToDurationCodec codec = StringToDurationCodec.INSTANCE;

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("1y3mo2h10m") // standard pattern
        .to(duration)
        .convertsFrom("P1Y3MT2H10M") // ISO 8601 pattern
        .to(duration)
        .convertsFrom("P0001-03-00T02:10:00") // alternative ISO 8601 pattern
        .to(duration)
        .convertsFrom("")
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec).convertsTo(duration).from("1y3mo2h10m").convertsTo(null).from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("1Y3M4D") // The minutes should be after days
        .cannotConvertFrom("not a valid duration");
  }
}
