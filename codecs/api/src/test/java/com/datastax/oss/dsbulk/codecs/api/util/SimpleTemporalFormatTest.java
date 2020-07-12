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
package com.datastax.oss.dsbulk.codecs.api.util;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class SimpleTemporalFormatTest {

  private final TemporalFormat localDateFormat = new SimpleTemporalFormat(ISO_LOCAL_DATE);

  private final TemporalFormat localTimeFormat = new SimpleTemporalFormat(ISO_LOCAL_TIME);

  @Test
  void should_parse_temporal() {
    assertThat(LocalDate.from(localDateFormat.parse("2018-02-01")))
        .isEqualTo(LocalDate.parse("2018-02-01"));
    assertThat(LocalTime.from(localTimeFormat.parse("13:24:59.123456789")))
        .isEqualTo(LocalTime.parse("13:24:59.123456789"));
    assertThat(LocalTime.from(localTimeFormat.parse("13:24:59")))
        .isEqualTo(LocalTime.parse("13:24:59"));
    assertThat(LocalTime.from(localTimeFormat.parse("13:24")))
        .isEqualTo(LocalTime.parse("13:24:00"));
  }

  @Test
  void should_format_temporal() {
    assertThat(localDateFormat.format(LocalDate.parse("2018-02-01"))).isEqualTo("2018-02-01");
    assertThat(localTimeFormat.format(LocalTime.parse("14:24:59.999"))).isEqualTo("14:24:59.999");
  }
}
