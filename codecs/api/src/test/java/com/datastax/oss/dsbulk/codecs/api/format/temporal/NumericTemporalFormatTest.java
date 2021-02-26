/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.api.format.temporal;

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
