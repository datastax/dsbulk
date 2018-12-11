/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.util.concurrent.FastThreadLocal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.Test;

class NumericTemporalFormatTest {

  private final Instant i = Instant.ofEpochMilli(1234);

  private final TemporalFormat format =
      new NumericTemporalFormat(
          new FastThreadLocal<NumberFormat>() {
            @Override
            protected NumberFormat initialValue() {
              return new DecimalFormat("#,###.##");
            }
          },
          UTC,
          MILLISECONDS,
          EPOCH.atZone(UTC));

  @Test
  void should_parse_numeric_temporal() {
    assertThat(format.parse(null)).isNull();
    assertThat(format.parse("")).isNull();
    assertThat(Instant.from(format.parse("1,234.00"))).isEqualTo(i);
    assertThatThrownBy(() -> format.parse("not a number"))
        .isInstanceOf(DateTimeParseException.class)
        .hasMessage("Could not parse temporal at index 0: not a number");
    assertThatThrownBy(() -> format.parse("1,234.00abcd"))
        .isInstanceOf(DateTimeParseException.class)
        .hasMessage("Could not parse temporal at index 8: 1,234.00abcd");
  }

  @Test
  void should_format_numeric_temporal() {
    assertThat(format.format(null)).isNull();
    assertThat(format.format(i)).isEqualTo("1,234");
  }
}
