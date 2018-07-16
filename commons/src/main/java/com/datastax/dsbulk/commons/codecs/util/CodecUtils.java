/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.util;

import static com.datastax.oss.driver.internal.core.util.Strings.unquote;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dse.driver.api.core.type.geometry.LineString;
import com.datastax.dse.driver.api.core.type.geometry.Point;
import com.datastax.dse.driver.api.core.type.geometry.Polygon;
import com.datastax.dse.driver.api.core.type.search.DateRange;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("WeakerAccess")
public class CodecUtils {

  private static final String CQL_TIMESTAMP = "CQL_TIMESTAMP";

  /**
   * Parses the given string as a temporal.
   *
   * <p>This method is more complex than {@link TemporalFormat#parse(String)} and targets CQL
   * timestamps specifically, and tries different approaches to infer the timestamp from the given
   * string.
   *
   * <p>It tries first to parse the string as an alphanumeric temporal, using the given parser; if
   * that fails, then it tries to parse it as a numeric temporal, using the given parser, time unit
   * and epoch.
   *
   * @param s the string to parse, may be {@code null}.
   * @param temporalFormat the parser to use if the string is alphanumeric; cannot be {@code null}.
   * @param numberFormat the parser to use if the string is numeric; cannot be {@code null}.
   * @param timeUnit the time unit to use if the string is numeric; cannot be {@code null}.
   * @param epoch the epoch to use if the string is numeric; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static TemporalAccessor parseTemporal(
      String s,
      @NotNull TemporalFormat temporalFormat,
      @NotNull NumberFormat numberFormat,
      @NotNull TimeUnit timeUnit,
      @NotNull Instant epoch) {
    Objects.requireNonNull(temporalFormat);
    Objects.requireNonNull(numberFormat);
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    if (s == null || s.isEmpty()) {
      return null;
    }
    TemporalAccessor temporal;
    try {
      // 1) try user-specified patterns
      temporal = temporalFormat.parse(s);
    } catch (DateTimeParseException e1) {
      try {
        // 2) try a number, then convert to instant since epoch
        Number number = parseNumber(s, numberFormat);
        // bypass overflow strategy, we don't want to alter timestamps
        temporal = numberToInstant(number, timeUnit, epoch);
      } catch (ParseException | ArithmeticException e2) {
        e2.addSuppressed(e1);
        IllegalArgumentException e3 =
            new IllegalArgumentException(
                String.format(
                    "Could not parse '%s'; accepted formats are: temporal string (e.g. '%s') or numeric value (%s since %s)",
                    s, Instant.now(), timeUnit.name().toLowerCase(), epoch));
        e3.addSuppressed(e2);
        throw e3;
      }
    }
    return temporal;
  }

  /**
   * Parses the given string as a number.
   *
   * <p>This method tries first to parse the string as a numeric value, using the given decimal
   * formatter; then, if that fails, it tries to parse it as an alphanumeric temporal, using the
   * given parser, and converts it to a numeric timestamp using the given time unit and the given
   * epoch; and if that fails too, it tries to convert it to a boolean number.
   *
   * @param s the string to parse, may be {@code null}.
   * @param numberFormat the {@link NumberFormat} to use to parse numbers; cannot be {@code null}.
   * @param temporalFormat the parser to use if the string is an alphanumeric temporal; cannot be
   *     {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @param timeUnit the time unit to use to convert the alphanumeric temporal to a numeric
   *     timestamp; cannot be {@code null}.
   * @param epoch the epoch to use to convert the alphanumeric temporal to a numeric timestamp;
   *     cannot be {@code null}.
   * @param booleanStrings A mapping between booleans and words.
   * @param booleanNumbers A mapping between booleans and numbers.
   * @return a {@link Number} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static Number parseNumber(
      String s,
      @NotNull NumberFormat numberFormat,
      @NotNull TemporalFormat temporalFormat,
      @NotNull ZoneId timeZone,
      @NotNull TimeUnit timeUnit,
      @NotNull ZonedDateTime epoch,
      @NotNull Map<String, Boolean> booleanStrings,
      @NotNull List<? extends Number> booleanNumbers) {
    Objects.requireNonNull(numberFormat);
    Objects.requireNonNull(temporalFormat);
    Objects.requireNonNull(timeZone);
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    Objects.requireNonNull(booleanStrings);
    Objects.requireNonNull(booleanNumbers);
    if (s == null || s.isEmpty()) {
      return null;
    }
    Number number;
    try {
      // 1) try user-specified patterns
      number = parseNumber(s, numberFormat);
    } catch (ParseException e1) {
      try {
        // 2) try new BigDecimal(s)
        number = new BigDecimal(s);
      } catch (NumberFormatException e2) {
        e2.addSuppressed(e1);
        try {
          // 3) try Double.valueOf(s)
          number = Double.valueOf(s);
        } catch (NumberFormatException e3) {
          e3.addSuppressed(e2);
          try {
            // 4) try a temporal, then convert to units since epoch
            TemporalAccessor temporal = temporalFormat.parse(s);
            assert temporal != null;
            Instant instant = toInstant(temporal, timeZone, epoch.toLocalDate());
            number = instantToNumber(instant, timeUnit, epoch.toInstant());
          } catch (DateTimeException e4) {
            // 5) Lastly, try a boolean word, then convert to number
            Boolean b = booleanStrings.get(s.toLowerCase());
            if (b != null) {
              number = booleanNumbers.get(b ? 0 : 1);
            } else {
              e4.addSuppressed(e3);
              IllegalArgumentException e5 =
                  new IllegalArgumentException(
                      String.format(
                          "Could not parse '%s'; accepted formats are: "
                              + "a valid number (e.g. '%s'), "
                              + "a valid Java numeric format (e.g. '-123.45e6'), "
                              + "a valid date-time pattern (e.g. '%s'), "
                              + "or a valid boolean word",
                          s,
                          formatNumber(1234.56, numberFormat),
                          temporalFormat.format(Instant.now())));
              e5.addSuppressed(e4);
              throw e5;
            }
          }
        }
      }
    }
    return number;
  }

  /**
   * Attempts to convert the given number to the target class, using loss-less conversions. If the
   * conversion fails, applies the overflow strategy and returns the narrowed number.
   *
   * @param <N> The target type.
   * @param value the value to convert.
   * @param targetClass the target class; cannot be {@code null}.
   * @param overflowStrategy the overflow strategy; cannot be {@code null}.
   * @param roundingMode the rounding mode; cannot be {@code null}.
   * @return the narrowed number.
   * @throws ArithmeticException if the number cannot be converted.
   */
  public static <N extends Number> N narrowNumber(
      Number value,
      @NotNull Class<? extends N> targetClass,
      @NotNull OverflowStrategy overflowStrategy,
      @NotNull RoundingMode roundingMode) {
    Objects.requireNonNull(targetClass);
    Objects.requireNonNull(overflowStrategy);
    Objects.requireNonNull(roundingMode);
    if (value == null) {
      return null;
    }
    try {
      return convertNumber(value, targetClass);
    } catch (ArithmeticException e1) {
      @SuppressWarnings("unchecked")
      N truncated = (N) overflowStrategy.apply(value, e1, targetClass, roundingMode);
      return truncated;
    }
  }

  /**
   * Converts the given number into an {@link Instant}, expressed in the given time unit, and
   * relative to the given epoch.
   *
   * @param n the number to parse, may be {@code null}.
   * @param timeUnit the time unit to use; cannot be {@code null}.
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return an {@link Instant} or {@code null} if the string was {@code null} or empty.
   * @throws ArithmeticException if the number cannot be converted to a Long.
   */
  public static Instant numberToInstant(
      Number n, @NotNull TimeUnit timeUnit, @NotNull Instant epoch) {
    if (n == null) {
      return null;
    }
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    Duration duration = Duration.ofNanos(NANOSECONDS.convert(toLongValueExact(n), timeUnit));
    return epoch.plus(duration);
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
  public static long instantToNumber(
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
   * Parses the given string using the given {@link NumberFormat}.
   *
   * @param s the string to parse, may be {@code null}.
   * @param format the format to use; cannot be {@code null}.
   * @return a {@link BigDecimal}, or {@code null} if the input was {@code null} or empty.
   * @throws ParseException if the string cannot be parsed.
   */
  public static Number parseNumber(String s, @NotNull NumberFormat format) throws ParseException {
    Objects.requireNonNull(format);
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    Number number = format.parse(s.trim(), pos);
    if (number == null) {
      throw new ParseException("Invalid number format: " + s, pos.getErrorIndex());
    }
    if (pos.getIndex() != s.length()) {
      throw new ParseException("Invalid number format: " + s, pos.getIndex());
    }
    return number;
  }

  /**
   * Formats the given number using the given format.
   *
   * @param value the value to format.
   * @param format the format to use; cannot be {@code null}.
   * @return the formatted value.
   * @throws NumberFormatException if the number cannot be formatted.
   */
  public static String formatNumber(Number value, @NotNull NumberFormat format)
      throws NumberFormatException {
    Objects.requireNonNull(format);
    if (value == null) {
      return null;
    }
    return format.format(value);
  }

  /**
   * Converts the given number to the given class, applying loss-less conversions.
   *
   * @param <N> the target type.
   * @param value the value to convert.
   * @param targetClass the target class; cannot be {@code null}.
   * @return the converted number.
   * @throws ArithmeticException if the number cannot be converted to the target class.
   */
  @SuppressWarnings("unchecked")
  public static <N extends Number> N convertNumber(
      Number value, @NotNull Class<? extends N> targetClass) throws ArithmeticException {
    Objects.requireNonNull(targetClass);
    if (value == null) {
      return null;
    }
    if (targetClass.equals(Byte.class)) {
      return (N) toByteValueExact(value);
    }
    if (targetClass.equals(Short.class)) {
      return (N) toShortValueExact(value);
    }
    if (targetClass.equals(Integer.class)) {
      return (N) toIntValueExact(value);
    }
    if (targetClass.equals(Long.class)) {
      return (N) toLongValueExact(value);
    }
    if (targetClass.equals(BigInteger.class)) {
      return (N) toBigIntegerExact(value);
    }
    if (targetClass.equals(Float.class)) {
      return (N) toFloatValueExact(value);
    }
    if (targetClass.equals(Double.class)) {
      return (N) toDoubleValueExact(value);
    }
    if (targetClass.equals(BigDecimal.class)) {
      return (N) toBigDecimal(value);
    }
    throw conversionFailed(value, targetClass);
  }

  /**
   * Converts the given number into a Byte, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   */
  public static Byte toByteValueExact(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof Byte) {
      return (Byte) value;
    } else if (value instanceof Short) {
      if (value.byteValue() != value.shortValue()) {
        throw conversionFailed(value, Byte.class);
      }
      return value.byteValue();
    } else if (value instanceof Integer) {
      if (value.byteValue() != value.intValue()) {
        throw conversionFailed(value, Byte.class);
      }
      return value.byteValue();
    } else if (value instanceof Long) {
      if (value.byteValue() != value.longValue()) {
        throw conversionFailed(value, Byte.class);
      }
      return value.byteValue();
    } else {
      try {
        if (value instanceof BigInteger) {
          return ((BigInteger) value).byteValueExact();
        } else if (value instanceof BigDecimal) {
          return ((BigDecimal) value).byteValueExact();
        } else {
          return new BigDecimal(value.toString()).byteValueExact();
        }
      } catch (ArithmeticException e) {
        throw conversionFailed(value, Byte.class, e);
      }
    }
  }

  /**
   * Converts the given number into a Short, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   */
  public static Short toShortValueExact(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof Short) {
      return (Short) value;
    } else if (value instanceof Byte) {
      return value.shortValue();
    } else if (value instanceof Integer) {
      if (value.shortValue() != value.intValue()) {
        throw conversionFailed(value, Short.class);
      }
      return value.shortValue();
    } else if (value instanceof Long) {
      if (value.shortValue() != value.longValue()) {
        throw conversionFailed(value, Short.class);
      }
      return value.shortValue();
    } else {
      try {
        if (value instanceof BigInteger) {
          return ((BigInteger) value).shortValueExact();
        } else if (value instanceof BigDecimal) {
          return ((BigDecimal) value).shortValueExact();
        } else {
          return new BigDecimal(value.toString()).shortValueExact();
        }
      } catch (ArithmeticException e) {
        throw conversionFailed(value, Short.class, e);
      }
    }
  }

  /**
   * Converts the given number into an Integer, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   */
  public static Integer toIntValueExact(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Byte || value instanceof Short) {
      return value.intValue();
    } else if (value instanceof Long) {
      if (value.intValue() != value.longValue()) {
        throw conversionFailed(value, Integer.class);
      }
      return value.intValue();
    } else {
      try {
        if (value instanceof BigInteger) {
          return ((BigInteger) value).intValueExact();
        } else if (value instanceof BigDecimal) {
          return ((BigDecimal) value).intValueExact();
        } else {
          return new BigDecimal(value.toString()).intValueExact();
        }
      } catch (ArithmeticException e) {
        throw conversionFailed(value, Integer.class, e);
      }
    }
  }

  /**
   * Converts the given number into a Long, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   */
  public static Long toLongValueExact(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      return value.longValue();
    } else {
      try {
        if (value instanceof BigInteger) {
          return ((BigInteger) value).longValueExact();
        } else if (value instanceof BigDecimal) {
          return ((BigDecimal) value).longValueExact();
        } else {
          return new BigDecimal(value.toString()).longValueExact();
        }
      } catch (ArithmeticException e) {
        throw conversionFailed(value, Long.class, e);
      }
    }
  }

  /**
   * Converts the given number into a BigInteger, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   */
  public static BigInteger toBigIntegerExact(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return BigInteger.valueOf(value.longValue());
    } else {
      try {
        if (value instanceof BigDecimal) {
          return ((BigDecimal) value).toBigIntegerExact();
        } else {
          return new BigDecimal(value.toString()).toBigIntegerExact();
        }
      } catch (ArithmeticException e) {
        throw conversionFailed(value, BigInteger.class, e);
      }
    }
  }

  /**
   * Converts the given number into a Float, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   * @throws NumberFormatException if the number cannot be converted to a {@link BigDecimal}.
   */
  public static Float toFloatValueExact(@NotNull Number value)
      throws ArithmeticException, NumberFormatException {
    Objects.requireNonNull(value);
    if (value instanceof Float) {
      return (Float) value;
    } else if (Float.isNaN(value.floatValue())) {
      return Float.NaN;
    } else if (Float.isInfinite(value.floatValue())) {
      if (value.doubleValue() == Double.NEGATIVE_INFINITY) {
        return Float.NEGATIVE_INFINITY;
      } else if (value.doubleValue() == Double.POSITIVE_INFINITY) {
        return Float.POSITIVE_INFINITY;
      }
      throw conversionFailed(value, Float.class);
    } else if (toBigDecimal(value).compareTo(new BigDecimal(Float.toString(value.floatValue())))
        != 0) {
      throw conversionFailed(value, Float.class);
    } else {
      return value.floatValue();
    }
  }

  /**
   * Converts the given number into a Double, throwing an exception in case of overflow.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException in case of overflow.
   * @throws NumberFormatException if the number cannot be converted to a {@link BigDecimal}.
   */
  public static Double toDoubleValueExact(@NotNull Number value)
      throws ArithmeticException, NumberFormatException {
    Objects.requireNonNull(value);
    if (value instanceof Double) {
      return (Double) value;
    } else if (Double.isNaN(value.doubleValue())) {
      return Double.NaN;
    } else if (Double.isInfinite(value.doubleValue())) {
      if (value.floatValue() == Float.NEGATIVE_INFINITY) {
        return Double.NEGATIVE_INFINITY;
      } else if (value.floatValue() == Float.POSITIVE_INFINITY) {
        return Double.POSITIVE_INFINITY;
      }
      throw conversionFailed(value, Double.class);
    } else if (toBigDecimal(value).compareTo(new BigDecimal(Double.toString(value.doubleValue())))
        != 0) {
      throw conversionFailed(value, Double.class);
    } else {
      return value.doubleValue();
    }
  }

  /**
   * Converts the given number into a BigDecimal.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws ArithmeticException if the number cannot be converted to a {@link BigDecimal}.
   */
  public static BigDecimal toBigDecimal(@NotNull Number value) throws ArithmeticException {
    Objects.requireNonNull(value);
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return BigDecimal.valueOf(value.longValue());
    } else if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    } else {
      try {
        return new BigDecimal(value.toString());
      } catch (NumberFormatException e) {
        throw conversionFailed(value, BigDecimal.class, e);
      }
    }
  }

  private static ArithmeticException conversionFailed(
      @NotNull Number value,
      @NotNull Class<? extends Number> targetClass,
      @NotNull Throwable cause) {
    return (ArithmeticException) conversionFailed(value, targetClass).initCause(cause);
  }

  private static ArithmeticException conversionFailed(
      @NotNull Number value, @NotNull Class<? extends Number> targetClass) {
    return new ArithmeticException(
        String.format(
            "Cannot convert %s from %s to %s",
            value, value.getClass().getSimpleName(), targetClass.getSimpleName()));
  }

  /**
   * Converts the given temporal into a temporal of the target class.
   *
   * @param <T> the target type.
   * @param value the value to convert.
   * @param targetClass the target class; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return the converted value.
   * @throws DateTimeException if the temporal cannot be converted to the target class.
   */
  @SuppressWarnings("unchecked")
  public static <T extends TemporalAccessor> T convertTemporal(
      TemporalAccessor value,
      @NotNull Class<? extends T> targetClass,
      @NotNull ZoneId timeZone,
      @NotNull LocalDate epoch)
      throws DateTimeException {
    Objects.requireNonNull(targetClass);
    Objects.requireNonNull(timeZone);
    Objects.requireNonNull(epoch);
    if (value == null) {
      return null;
    }
    if (targetClass.equals(LocalDate.class)) {
      return (T) toLocalDate(value, timeZone);
    }
    if (targetClass.equals(LocalTime.class)) {
      return (T) toLocalTime(value, timeZone);
    }
    if (targetClass.equals(LocalDateTime.class)) {
      return (T) toLocalDateTime(value, timeZone, epoch);
    }
    if (targetClass.equals(Instant.class)) {
      return (T) toInstant(value, timeZone, epoch);
    }
    if (targetClass.equals(ZonedDateTime.class)) {
      return (T) toZonedDateTime(value, timeZone, epoch);
    }
    throw new DateTimeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetClass));
  }

  /**
   * Converts the given temporal into a {@link ZonedDateTime}.
   *
   * @param value the value to convert; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return the converted value.
   * @throws DateTimeException if the temporal cannot be converted.
   */
  public static ZonedDateTime toZonedDateTime(
      @NotNull TemporalAccessor value, @NotNull ZoneId timeZone, @NotNull LocalDate epoch)
      throws DateTimeException {
    Objects.requireNonNull(value);
    Objects.requireNonNull(timeZone);
    Objects.requireNonNull(epoch);
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay(timeZone);
    }
    if (value instanceof LocalTime) {
      return ((LocalTime) value).atDate(epoch).atZone(timeZone);
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).atZone(timeZone);
    }
    if (value instanceof Instant) {
      return ((Instant) value).atZone(timeZone);
    }
    if (value instanceof ZonedDateTime) {
      return (ZonedDateTime) value;
    }
    // if the temporal contains a zone, use that,
    // otherwise, use the default zone
    if (value.query(TemporalQueries.zone()) != null) {
      return ZonedDateTime.from(value);
    }
    return Instant.from(value).atZone(timeZone);
  }

  /**
   * Converts the given temporal into an {@link Instant}.
   *
   * @param value the value to convert; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return the converted value.
   * @throws DateTimeException if the temporal cannot be converted.
   */
  public static Instant toInstant(
      @NotNull TemporalAccessor value, @NotNull ZoneId timeZone, @NotNull LocalDate epoch)
      throws DateTimeException {
    Objects.requireNonNull(value);
    Objects.requireNonNull(timeZone);
    Objects.requireNonNull(epoch);
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay(timeZone).toInstant();
    }
    if (value instanceof LocalTime) {
      return ((LocalTime) value).atDate(epoch).atZone(timeZone).toInstant();
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).atZone(timeZone).toInstant();
    }
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toInstant();
    }
    return Instant.from(value);
  }

  /**
   * Converts the given temporal into a {@link LocalDateTime}.
   *
   * @param value the value to convert; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @param epoch the epoch to use; cannot be {@code null}.
   * @return the converted instant.
   * @throws DateTimeException if the temporal cannot be converted.
   */
  public static LocalDateTime toLocalDateTime(
      @NotNull TemporalAccessor value, @NotNull ZoneId timeZone, @NotNull LocalDate epoch)
      throws DateTimeException {
    Objects.requireNonNull(value);
    Objects.requireNonNull(timeZone);
    Objects.requireNonNull(epoch);
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay();
    }
    if (value instanceof LocalTime) {
      return ((LocalTime) value).atDate(epoch);
    }
    if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    if (value instanceof Instant) {
      return ((Instant) value).atZone(timeZone).toLocalDateTime();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toLocalDateTime();
    }
    return LocalDateTime.from(value);
  }

  /**
   * Converts the given temporal into a {@link LocalDate}.
   *
   * @param value the value to convert; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @return the converted instant.
   * @throws DateTimeException if the temporal cannot be converted.
   */
  public static LocalDate toLocalDate(@NotNull TemporalAccessor value, @NotNull ZoneId timeZone)
      throws DateTimeException {
    Objects.requireNonNull(value);
    Objects.requireNonNull(timeZone);
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalDate();
    }
    if (value instanceof Instant) {
      return ((Instant) value).atZone(timeZone).toLocalDate();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toLocalDate();
    }
    return LocalDate.from(value);
  }

  /**
   * Converts the given temporal into a {@link LocalTime}.
   *
   * @param value the value to convert; cannot be {@code null}.
   * @param timeZone the time zone to use; cannot be {@code null}.
   * @return the converted instant.
   * @throws DateTimeException if the temporal cannot be converted.
   */
  public static LocalTime toLocalTime(@NotNull TemporalAccessor value, @NotNull ZoneId timeZone)
      throws DateTimeException {
    Objects.requireNonNull(value);
    Objects.requireNonNull(timeZone);
    if (value instanceof LocalTime) {
      return (LocalTime) value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalTime();
    }
    if (value instanceof Instant) {
      return ((Instant) value).atZone(timeZone).toLocalTime();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toLocalTime();
    }
    return LocalTime.from(value);
  }

  /**
   * Parses the given string as a {@link UUID}.
   *
   * <p>First, if the string is a UUID in canonical representation, parses the string using {@link
   * UUID#fromString(String)}. If that fails, then tries to parse the string as a temporal, then
   * converts the temporal into a time UUID.
   *
   * @param s the string to parse; may be {@code null}.
   * @param instantCodec the codec to use to parse temporals; cannot be {@code null}.
   * @param generator the generator to use to create a time UUIDl cannot be {@code null}.
   * @return the parsed UUID.
   */
  public static UUID parseUUID(
      String s,
      @NotNull ConvertingCodec<String, Instant> instantCodec,
      @NotNull TimeUUIDGenerator generator) {
    Objects.requireNonNull(instantCodec);
    Objects.requireNonNull(generator);
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return UUID.fromString(s);
    } catch (IllegalArgumentException e1) {
      Instant instant;
      try {
        instant = instantCodec.externalToInternal(s);
      } catch (Exception e2) {
        e2.addSuppressed(e1);
        throw new IllegalArgumentException("Invalid UUID string: " + s, e2);
      }
      return generator.generate(instant);
    }
  }

  /**
   * Parses the given string as a {@link ByteBuffer}.
   *
   * <p>First, tries to parse the string as a CQL blob literal in hexadecimal notation; if that
   * fails, then tries to parse the string as a Base64-encoded byte array.
   *
   * @param s the string to parse; may be {@code null}.
   * @return the parsed {@link ByteBuffer}.
   */
  public static ByteBuffer parseByteBuffer(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return Bytes.fromHexString(s);
    } catch (IllegalArgumentException e) {
      try {
        return ByteBuffer.wrap(Base64.getDecoder().decode(s));
      } catch (IllegalArgumentException e1) {
        e1.addSuppressed(e);
        throw new IllegalArgumentException("Invalid binary string: " + s, e1);
      }
    }
  }

  public static Point parsePoint(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      // CQL geometry literals are quoted, but WKT strings are not,
      // so we accept both
      return Point.fromWellKnownText(unquote(s));
    } catch (Exception e) {
      try {
        return Point.fromGeoJson(s);
      } catch (Exception e1) {
        e1.addSuppressed(e);
        throw new IllegalArgumentException("Invalid point literal: " + s, e1);
      }
    }
  }

  public static LineString parseLineString(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      // CQL geometry literals are quoted, but WKT strings are not,
      // so we accept both
      return LineString.fromWellKnownText(unquote(s));
    } catch (Exception e) {
      try {
        return LineString.fromGeoJson(s);
      } catch (Exception e1) {
        e1.addSuppressed(e);
        throw new IllegalArgumentException("Invalid line string literal: " + s, e1);
      }
    }
  }

  public static Polygon parsePolygon(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      // CQL geometry literals are quoted, but WKT strings are not,
      // so we accept both
      return Polygon.fromWellKnownText(unquote(s));
    } catch (Exception e) {
      try {
        return Polygon.fromGeoJson(s);
      } catch (Exception e1) {
        e1.addSuppressed(e);
        throw new IllegalArgumentException("Invalid polygon literal: " + s, e1);
      }
    }
  }

  public static DateRange parseDateRange(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return DateRange.parse(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid date range literal: " + s, e);
    }
  }

  public static FastThreadLocal<NumberFormat> getNumberFormatThreadLocal(
      String pattern, Locale locale, RoundingMode roundingMode, boolean formatNumbers) {
    return new FastThreadLocal<NumberFormat>() {
      @Override
      protected NumberFormat initialValue() {
        return getNumberFormat(pattern, locale, roundingMode, formatNumbers);
      }
    };
  }

  public static NumberFormat getNumberFormat(
      String pattern, Locale locale, RoundingMode roundingMode, boolean formatNumbers) {
    DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(locale);
    // manually set the NaN and Infinity symbols; the default ones are not parseable:
    // 'REPLACEMENT CHARACTER' (U+FFFD) and 'INFINITY' (U+221E)
    symbols.setNaN("NaN");
    symbols.setInfinity("Infinity");
    DecimalFormat format = new DecimalFormat(pattern, symbols);
    // Always parse floating point numbers as BigDecimals to preserve maximum precision
    format.setParseBigDecimal(true);
    // Used only when formatting
    format.setRoundingMode(roundingMode);
    if (roundingMode == RoundingMode.UNNECESSARY) {
      // if user selects unnecessary, print as many fraction digits as necessary
      format.setMaximumFractionDigits(Integer.MAX_VALUE);
    }
    if (!formatNumbers) {
      return new ToStringNumberFormat(format);
    } else {
      return new ExactNumberFormat(format);
    }
  }

  @VisibleForTesting
  public static TemporalFormat getTemporalFormat(String pattern, ZoneId timeZone, Locale locale) {
    if (pattern.equals(CQL_TIMESTAMP)) {
      return new CqlTemporalFormat(timeZone);
    } else {
      DateTimeFormatterBuilder builder =
          new DateTimeFormatterBuilder().parseStrict().parseCaseInsensitive();
      try {
        // first, assume it is a predefined format
        Field field = DateTimeFormatter.class.getDeclaredField(pattern);
        DateTimeFormatter formatter = (DateTimeFormatter) field.get(null);
        builder = builder.append(formatter);
      } catch (NoSuchFieldException | IllegalAccessException ignored) {
        // if that fails, assume it's a pattern
        builder = builder.appendPattern(pattern);
      }
      DateTimeFormatter format =
          builder
              .toFormatter(locale)
              // STRICT fails sometimes, e.g. when extracting the Year field from a YearOfEra field
              // (i.e., does not convert between "uuuu" and "yyyy")
              .withResolverStyle(ResolverStyle.SMART)
              .withChronology(IsoChronology.INSTANCE);
      if (timeZone == null) {
        return new SimpleTemporalFormat(format);
      } else {
        return new ZonedTemporalFormat(format, timeZone);
      }
    }
  }
}
