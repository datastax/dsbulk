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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

/**
 * A special temporal format that applies a fixed time zone to parsed inputs when they do not
 * contain any time zone or time offset information, thus allowing such inputs to be converted to an
 * {@link Instant}.
 */
public class ZonedTemporalFormat extends SimpleTemporalFormat {

  private final ZoneId timeZone;

  public ZonedTemporalFormat(DateTimeFormatter parserAndFormatter, ZoneId timeZone) {
    this(parserAndFormatter, parserAndFormatter, timeZone);
  }

  public ZonedTemporalFormat(
      DateTimeFormatter parser, DateTimeFormatter formatter, ZoneId timeZone) {
    // Always force a time zone when formatting (makes formatting of unzoned input possible),
    // but never set a time zone when parsing (dangerous as original zone info could be overridden
    // and data altered)
    super(parser, formatter.withZone(timeZone));
    Preconditions.checkState(parser.getZone() == null, "parser should not have an override zone");
    this.timeZone = timeZone;
  }

  @Override
  public TemporalAccessor parse(String text) {
    TemporalAccessor temporal = super.parse(text);
    if (temporal == null) {
      return null;
    }
    // If an instant cannot be inferred from the input (possibly because the input did not
    // contain any time zone information), try extracting the local date/time from the input and
    // apply the default time zone.
    if (!temporal.isSupported(ChronoField.INSTANT_SECONDS)) {
      return ZonedDateTime.of(
          temporal.query(TemporalQueries.localDate()),
          temporal.query(TemporalQueries.localTime()),
          timeZone);
    } else {
      return temporal;
    }
  }
}
