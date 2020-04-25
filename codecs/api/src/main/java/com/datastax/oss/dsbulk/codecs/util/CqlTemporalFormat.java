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

import static java.time.format.TextStyle.SHORT;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.Locale.US;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;

/**
 * A special zoned temporal format that recognizes all valid CQL input formats when parsing.
 *
 * <p>When formatting, this format uses {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME} as the
 * formatting pattern, which is compliant with both CQL and ISO-8601.
 */
public class CqlTemporalFormat extends ZonedTemporalFormat {

  public static final CqlTemporalFormat DEFAULT_INSTANCE = new CqlTemporalFormat(ZoneId.of("UTC"));

  public CqlTemporalFormat(@NonNull ZoneId timeZone) {
    super(createParser(), createFormatter(timeZone), timeZone);
  }

  @NonNull
  private static DateTimeFormatter createParser() {
    // this formatter is a hybrid parser that combines all valid CQL patterns declared in C* 2.2+
    // into a single parser. To achieve that we "cheat" a little bit and accept many optional
    // components that would not make sense together. For example, we accept both 'T' and blank as
    // date-time separators, so in theory we also accept "T " (i.e. 'T' followed by a blank).
    return new DateTimeFormatterBuilder()

        // date part
        .append(DateTimeFormatter.ISO_LOCAL_DATE)

        // date-time separators
        .optionalStart()
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()

        // time part, includes fraction of second
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalEnd()

        // time zone part

        // the following corresponds to z, zz, and zzz in the C* patterns (they are equivalent),
        // preceded by a blank
        .optionalStart()
        .appendLiteral(' ')
        .appendZoneText(SHORT)
        .optionalEnd()

        // the following corresponds to X, XX, and XXX in the C* patterns
        .optionalStart()
        .appendOffset("+HH:MM", "Z") // XXX, matches +02:00
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHmm", "Z") // X or XX, matches +0200 and +02
        .optionalEnd()

        // add defaults for missing fields, which allows the parsing to be lenient when
        // some fields cannot be inferred from the input.
        .parseDefaulting(HOUR_OF_DAY, 0)
        .parseDefaulting(MINUTE_OF_HOUR, 0)
        .parseDefaulting(SECOND_OF_MINUTE, 0)
        .parseDefaulting(NANO_OF_SECOND, 0)
        .toFormatter()
        .withLocale(US)
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE);
  }

  @NonNull
  private static DateTimeFormatter createFormatter(ZoneId timeZone) {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .withLocale(US)
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE)
        .withZone(timeZone);
  }
}
