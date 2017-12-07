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
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class StringToBigIntegerCodecTest {

  private final StringToBigIntegerCodec codec =
      new StringToBigIntegerCodec(
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
        .to(BigInteger.ZERO)
        .convertsFrom("-1,234")
        .to(new BigInteger("-1234"))
        .convertsFrom("1970-01-01T00:00:00Z")
        .to(new BigInteger("0"))
        .convertsFrom("2000-01-01T00:00:00Z")
        .to(new BigInteger("946684800000"))
        .convertsFrom("TRUE")
        .to(BigInteger.ONE)
        .convertsFrom("FALSE")
        .to(BigInteger.ZERO)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(BigInteger.ZERO)
        .from("0")
        .convertsTo(new BigInteger("-1234"))
        .from("-1,234")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("-1.234").cannotConvertFrom("not a valid biginteger");
  }
}
