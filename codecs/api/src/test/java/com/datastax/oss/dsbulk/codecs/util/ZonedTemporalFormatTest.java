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
package com.datastax.oss.dsbulk.codecs.util;

import static java.time.ZoneOffset.ofHours;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class ZonedTemporalFormatTest {

  private final Instant i = Instant.parse("2017-11-23T12:24:59Z");

  private final TemporalFormat format1 =
      new ZonedTemporalFormat(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"), ofHours(2));
  private final TemporalFormat format2 =
      new ZonedTemporalFormat(DateTimeFormatter.ofPattern("yyyyMMddHHmmss[XXX]"), ofHours(2));

  @Test
  void should_parse_temporal() {
    assertThat(format1.parse(null)).isNull();
    assertThat(format1.parse("")).isNull();
    assertThat(Instant.from(format1.parse("20171123142459"))).isEqualTo(i);
    assertThat(Instant.from(format2.parse("20171123142459"))).isEqualTo(i);
    assertThat(Instant.from(format2.parse("20171123142459+02:00"))).isEqualTo(i);
    assertThat(Instant.from(format2.parse("20171123152459+03:00"))).isEqualTo(i);
    assertThat(Instant.from(format2.parse("20171123202459+08:00"))).isEqualTo(i);
  }

  @Test
  void should_format_temporal() {
    assertThat(format1.format(Instant.parse("2017-11-23T14:24:59.999Z")))
        .isEqualTo("20171123162459"); // at +02:00
    assertThat(format2.format(Instant.parse("2017-11-23T14:24:59.999Z")))
        .isEqualTo("20171123162459+02:00");
  }
}
