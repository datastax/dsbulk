/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/** */
public class CodecUtils {

  /**
   * Parses the given string as a temporal.
   *
   * <p>This method tries first to parse the string as an alphanumeric temporal, using the given
   * parser; if that fails, then it tries to parse it as a numeric temporal, using the given time
   * unit and the given epoch.
   *
   * @param s the string to parse, may be {@code null}.
   * @param parser the parser to use if the string is alphanumeric; cannot be {@code null}.
   * @param numericTimestampUnit the time unit to use if the string is numeric; cannot be {@code
   *     null}.
   * @param numericTimestampEpoch the epoch to use if the string is numeric; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static TemporalAccessor parseTemporal(
      String s,
      @NotNull DateTimeFormatter parser,
      @NotNull TimeUnit numericTimestampUnit,
      @NotNull Instant numericTimestampEpoch) {
    Objects.requireNonNull(parser);
    Objects.requireNonNull(numericTimestampUnit);
    Objects.requireNonNull(numericTimestampEpoch);
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return parseTemporal(s, parser);
    } catch (Exception e1) {
      try {
        return parseInstant(s, numericTimestampUnit, numericTimestampEpoch);
      } catch (Exception e2) {
        e2.addSuppressed(e1);
        IllegalArgumentException e3 =
            new IllegalArgumentException(
                String.format(
                    "Could not parse '%s'; accepted formats are: %s since %s or a valid date-time pattern (e.g. '%s')",
                    s,
                    numericTimestampUnit.name().toLowerCase(),
                    numericTimestampEpoch,
                    Instant.now()));
        e3.addSuppressed(e2);
        throw e3;
      }
    }
  }

  /**
   * Parses the given string as an alphanumeric temporal, using the given parser.
   *
   * @param s the string to parse, may be {@code null}.
   * @param parser teh parser; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static TemporalAccessor parseTemporal(String s, @NotNull DateTimeFormatter parser) {
    Objects.requireNonNull(parser);
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    TemporalAccessor accessor = parser.parse(s, pos);
    if (pos.getIndex() != s.length()) {
      throw new IllegalArgumentException(
          "Cannot parse temporal: " + s, new ParseException(s, pos.getErrorIndex()));
    }
    return accessor;
  }

  /**
   * Parses the given string as a numeric temporal, expressed in the given time unit, and relative
   * to the given epoch.
   *
   * @param s the string to parse, may be {@code null}.
   * @param timeUnit the time unit to use if the string is numeric; cannot be {@code null}.
   * @param epoch the epoch to use if the string is numeric; cannot be {@code null}.
   * @return an {@link Instant} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static Instant parseInstant(String s, @NotNull TimeUnit timeUnit, @NotNull Instant epoch) {
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      Duration duration = Duration.ofNanos(NANOSECONDS.convert(Long.parseLong(s), timeUnit));
      return epoch.plus(duration);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Cannot parse numeric temporal: " + s, e);
    }
  }

  /**
   * Converts the given {@link Instant} into a scalar timestamp expressed in the given time unit and
   * relative to the Unix {@link Instant#EPOCH Epoch}.
   *
   * @param instant the instant to convert; cannot be {@code null}.
   * @param timeUnit the time unit to use; cannot be {@code null}.
   * @return a long representing the number of time units since the Epoch.
   */
  @NotNull
  public static long instantToTimestampSinceEpoch(
      @NotNull Instant instant, @NotNull TimeUnit timeUnit) {
    Objects.requireNonNull(instant);
    Objects.requireNonNull(timeUnit);
    return timeUnit.convert(instant.getEpochSecond(), SECONDS)
        + timeUnit.convert(instant.getNano(), NANOSECONDS);
  }
}
