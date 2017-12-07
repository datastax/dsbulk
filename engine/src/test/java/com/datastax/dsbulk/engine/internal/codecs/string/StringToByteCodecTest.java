/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class StringToByteCodecTest {

  private final StringToByteCodec codec =
      new StringToByteCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to((byte) 0)
        .convertsFrom("127")
        .to((byte) 127)
        .convertsFrom("-128")
        .to((byte) -128)
        .convertsFrom("0")
        .to((byte) 0)
        .convertsFrom("127")
        .to((byte) 127)
        .convertsFrom("-128")
        .to((byte) -128)
        .convertsFrom("1970-01-01T00:00:00Z")
        .to((byte) 0)
        .convertsFrom("TRUE")
        .to((byte) 1)
        .convertsFrom("FALSE")
        .to((byte) 0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo((byte) 0)
        .from("0")
        .convertsTo((byte) 127)
        .from("127")
        .convertsTo((byte) -128)
        .from("-128")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid byte")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("128")
        .cannotConvertFrom("-129")
        .cannotConvertFrom("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
