/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
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
   * Parses the given string as a number.
   *
   * <p>This method tries first to parse the string as a numeric value, using the given decimal
   * formatter; then, if that fails, it tries to parse it as an alphanumeric temporal, using the
   * given parser, and converts it to a numeric timestamp using the given time unit and the given
   * epoch.
   *
   * @param s the string to parse, may be {@code null}.
   * @param formatter the {@link DecimalFormat} to use to parse numbers; cannot be {@code null}.
   * @param parser the parser to use if the string is an alphanumeric temporal; cannot be {@code
   *     null}.
   * @param numericTimestampUnit the time unit to use to convert the alphanumeric temporal to a
   *     numeric timestamp; cannot be {@code null}.
   * @param numericTimestampEpoch the epoch to use to convert the alphanumeric temporal to a numeric
   *     timestamp; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static Number parseNumber(
      String s,
      @NotNull DecimalFormat formatter,
      @NotNull DateTimeFormatter parser,
      @NotNull TimeUnit numericTimestampUnit,
      @NotNull Instant numericTimestampEpoch) {
    Objects.requireNonNull(formatter);
    Objects.requireNonNull(parser);
    Objects.requireNonNull(numericTimestampUnit);
    Objects.requireNonNull(numericTimestampEpoch);
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return parseBigDecimal(s, formatter);
    } catch (Exception e1) {
      try {
        TemporalAccessor temporal = parseTemporal(s, parser);
        assert temporal != null;
        Instant instant = Instant.from(temporal);
        return instantToTimestampSinceEpoch(instant, numericTimestampUnit, numericTimestampEpoch);
      } catch (Exception e2) {
        e2.addSuppressed(e1);
        IllegalArgumentException e3 =
            new IllegalArgumentException(
                String.format(
                    "Could not parse '%s'; accepted formats are: a valid number (e.g. '%s') or a valid date-time pattern (e.g. '%s')",
                    s, 1234.56, Instant.now()));
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
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return a long representing the number of time units since the given epoch.
   */
  public static long instantToTimestampSinceEpoch(
      @NotNull Instant instant, @NotNull TimeUnit timeUnit, @NotNull Instant epoch) {
    Objects.requireNonNull(instant);
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    long t1 =
        timeUnit.convert(instant.getEpochSecond(), SECONDS)
            + timeUnit.convert(instant.getNano(), NANOSECONDS);
    long t0 =
        timeUnit.convert(epoch.getEpochSecond(), SECONDS)
            + timeUnit.convert(epoch.getNano(), NANOSECONDS);
    return t1 - t0;
  }

  /**
   * Parses the given string into a {@link BigDecimal} using the given {@link DecimalFormat}.
   *
   * @param s the string to parse, may be {@code null}.
   * @param formatter the formatter to use; cannot be {@code null}.
   * @return a {@link BigDecimal}, or {@code null} if the input was {@code null} or empty.
   */
  public static BigDecimal parseBigDecimal(String s, @NotNull DecimalFormat formatter) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    BigDecimal number = (BigDecimal) formatter.parse(s.trim(), pos);
    if (number == null) {
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getErrorIndex()));
    }
    if (pos.getIndex() != s.length()) {
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getIndex()));
    }
    return number;
  }

  public static byte toByteValueExact(Number value) {
    if (value instanceof Byte) {
      return (byte) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).byteValueExact();
    } else {
      return new BigDecimal(value.toString()).byteValueExact();
    }
  }

  public static short toShortValueExact(Number value) {
    if (value instanceof Short) {
      return (short) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).shortValueExact();
    } else {
      return new BigDecimal(value.toString()).shortValueExact();
    }
  }

  public static int toIntValueExact(Number value) {
    if (value instanceof Integer) {
      return (int) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).intValueExact();
    } else {
      return new BigDecimal(value.toString()).intValueExact();
    }
  }

  public static long toLongValueExact(Number value) {
    if (value instanceof Long) {
      return (long) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).longValueExact();
    } else {
      return new BigDecimal(value.toString()).longValueExact();
    }
  }

  public static float toFloatValue(Number value) {
    if (value instanceof Float) {
      return (float) value;
    } else {
      return value.floatValue();
    }
  }

  public static double toDoubleValue(Number value) {
    if (value instanceof Double) {
      return (double) value;
    } else {
      return value.doubleValue();
    }
  }

  public static BigInteger toBigIntegerExact(Number value) {
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toBigIntegerExact();
    } else {
      return new BigDecimal(value.toString()).toBigIntegerExact();
    }
  }

  public static BigDecimal toBigDecimal(Number value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else {
      return new BigDecimal(value.toString());
    }
  }
}
