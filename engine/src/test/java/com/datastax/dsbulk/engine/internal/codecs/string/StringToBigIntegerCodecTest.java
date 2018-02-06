/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class StringToBigIntegerCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final StringToBigIntegerCodec codec =
      new StringToBigIntegerCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() {
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
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(BigInteger.ZERO)
        .from("0")
        .convertsTo(new BigInteger("-1234"))
        .from("-1,234")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("-1.234").cannotConvertFrom("not a valid biginteger");
  }
}
