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
package com.datastax.oss.dsbulk.commons;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DurationUtils {

  /**
   * Rounds the given duration to the given time unit.
   *
   * <p>Conversions from finer to coarser granularities may result in precision loss; in such cases,
   * rounding is done towards the floor. For example, rounding 119 seconds to minutes will result in
   * one minute.
   *
   * @param duration The duration to round.
   * @param unit The unit to round to.
   * @return A rounded duration.
   */
  @NonNull
  public static Duration round(@NonNull Duration duration, @NonNull TimeUnit unit) {
    long nanos = duration.toNanos();
    long converted = unit.convert(nanos, TimeUnit.NANOSECONDS);
    return Duration.ofNanos(unit.toNanos(converted));
  }

  /**
   * Formats the given duration in plain English text.
   *
   * @param duration The duration to format.
   * @return The duration expressed in plain English.
   */
  @NonNull
  public static String formatDuration(@NonNull Duration duration) {
    if (duration.isNegative()) {
      throw new IllegalArgumentException("Cannot format negative duration");
    }
    long days = duration.toDays();
    duration = duration.minusDays(days);
    long hours = duration.toHours();
    duration = duration.minusHours(hours);
    long minutes = duration.toMinutes();
    duration = duration.minusMinutes(minutes);
    long seconds = duration.getSeconds();
    duration = duration.minusSeconds(seconds);
    long millis = duration.toMillis();
    duration = duration.minusMillis(millis);
    long nanos = duration.getNano();
    List<StringBuilder> segments = new ArrayList<>();
    maybeAppendSegment(segments, days, "day");
    maybeAppendSegment(segments, hours, "hour");
    maybeAppendSegment(segments, minutes, "minute");
    maybeAppendSegment(segments, seconds, "second");
    maybeAppendSegment(segments, millis, "millisecond");
    maybeAppendSegment(segments, nanos, "nanosecond");
    StringBuilder sb = new StringBuilder();
    Iterator<StringBuilder> it = segments.iterator();
    while (it.hasNext()) {
      StringBuilder segment = it.next();
      if (sb.length() > 0) {
        if (it.hasNext()) {
          sb.append(", ");
        } else {
          sb.append(" and ");
        }
      }
      sb.append(segment);
    }
    return sb.toString();
  }

  private static void maybeAppendSegment(List<StringBuilder> segments, long amount, String label) {
    if (amount > 0) {
      StringBuilder sb =
          new StringBuilder().append(String.format("%,d", amount)).append(' ').append(label);
      if (amount > 1) {
        sb.append('s');
      }
      segments.add(sb);
    }
  }
}
