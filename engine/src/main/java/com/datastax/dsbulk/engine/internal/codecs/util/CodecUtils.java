/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class CodecUtils {

  /**
   * Parses the given string as a temporal.
   *
   * <p>This method is more complex than {@link #parseTemporal(String, DateTimeFormatter)} and
   * targets CQL timestamps specifically, and tries different approaches to infer the timestamp from
   * the given string.
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
      @NotNull DateTimeFormatter temporalFormat,
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
      temporal = parseTemporal(s, temporalFormat);
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
   * @param timeUnit the time unit to use to convert the alphanumeric temporal to a numeric
   *     timestamp; cannot be {@code null}.
   * @param epoch the epoch to use to convert the alphanumeric temporal to a numeric timestamp;
   *     cannot be {@code null}.
   * @param booleanWords A mapping between booleans and words.
   * @param booleanNumbers A mapping between booleans and numbers.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static Number parseNumber(
      String s,
      @NotNull NumberFormat numberFormat,
      @NotNull DateTimeFormatter temporalFormat,
      @NotNull TimeUnit timeUnit,
      @NotNull ZonedDateTime epoch,
      @NotNull Map<String, Boolean> booleanWords,
      @NotNull List<? extends Number> booleanNumbers) {
    Objects.requireNonNull(numberFormat);
    Objects.requireNonNull(temporalFormat);
    Objects.requireNonNull(timeUnit);
    Objects.requireNonNull(epoch);
    Objects.requireNonNull(booleanWords);
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
        // 2) try Double.valueOf(s)
        number = Double.valueOf(s);
      } catch (NumberFormatException e2) {
        e2.addSuppressed(e1);
        try {
          // 3) try a temporal, then convert to units since epoch
          TemporalAccessor temporal = parseTemporal(s, temporalFormat);
          assert temporal != null;
          Instant instant = toInstant(temporal, temporalFormat.getZone(), epoch.toLocalDate());
          number = instantToNumber(instant, timeUnit, epoch.toInstant());
        } catch (DateTimeException e3) {
          // 4) Lastly, try a boolean word, then convert to number
          Boolean b = booleanWords.get(s.toLowerCase());
          if (b != null) {
            number = booleanNumbers.get(b ? 0 : 1);
          } else {
            e3.addSuppressed(e2);
            IllegalArgumentException e4 =
                new IllegalArgumentException(
                    String.format(
                        "Could not parse '%s'; accepted formats are: "
                            + "a valid number (e.g. '%s'), "
                            + "a valid Java numeric format (e.g. '-123.45e6'), "
                            + "a valid date-time pattern (e.g. '%s'), "
                            + "or a valid boolean word",
                        s,
                        formatNumber(1234.56, numberFormat),
                        formatTemporal(Instant.now(), temporalFormat)));
            e4.addSuppressed(e3);
            throw e4;
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
   * @throws IllegalArgumentException if the number cannot be converted.
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
      try {
        @SuppressWarnings("unchecked")
        N truncated = (N) overflowStrategy.apply(value, e1, targetClass, roundingMode);
        return truncated;
      } catch (ArithmeticException e2) {
        // e2 should be the same as e1, rethrown
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert %s of type %s to %s", value, value.getClass(), targetClass),
            e2);
      }
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
   * Parses the given string as an alphanumeric temporal, using the given parser.
   *
   * @param s the string to parse, may be {@code null}.
   * @param format the format to use; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws DateTimeParseException if the string cannot be parsed.
   */
  public static TemporalAccessor parseTemporal(String s, @NotNull DateTimeFormatter format)
      throws DateTimeParseException {
    Objects.requireNonNull(format);
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    TemporalAccessor accessor = format.parse(s.trim(), pos);
    if (pos.getIndex() != s.length()) {
      throw new DateTimeParseException("Invalid temporal format", s, pos.getIndex());
    }
    return accessor;
  }

  /**
   * Formats the given temporal using the given format.
   *
   * @param value the value to format.
   * @param format the format to use; cannot be {@code null}.
   * @return the formatted value.
   * @throws DateTimeException if the value cannot be formatted.
   */
  public static String formatTemporal(TemporalAccessor value, @NotNull DateTimeFormatter format)
      throws DateTimeException {
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
   * @throws IllegalArgumentException if the target class is unknown.
   * @throws ArithmeticException if he number cannot be converted to the target class without
   *     precision loss.
   * @throws NumberFormatException if the number cannot be converted to the target class.
   */
  @SuppressWarnings("unchecked")
  public static <N> N convertNumber(Number value, @NotNull Class<? extends N> targetClass)
      throws IllegalArgumentException, ArithmeticException {
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
    throw new IllegalArgumentException(
        String.format("Cannot convert %s of %s to %s", value, value.getClass(), targetClass));
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
        throw new ArithmeticException("integer overflow");
      }
      return value.byteValue();
    } else if (value instanceof Integer) {
      if (value.byteValue() != value.intValue()) {
        throw new ArithmeticException("integer overflow");
      }
      return value.byteValue();
    } else if (value instanceof Long) {
      if (value.byteValue() != value.longValue()) {
        throw new ArithmeticException("integer overflow");
      }
      return value.byteValue();
    } else if (value instanceof BigInteger) {
      return ((BigInteger) value).byteValueExact();
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).byteValueExact();
    } else {
      return new BigDecimal(value.toString()).byteValueExact();
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
        throw new ArithmeticException("integer overflow");
      }
      return value.shortValue();
    } else if (value instanceof Long) {
      if (value.shortValue() != value.longValue()) {
        throw new ArithmeticException("integer overflow");
      }
      return value.shortValue();
    } else if (value instanceof BigInteger) {
      return ((BigInteger) value).shortValueExact();
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).shortValueExact();
    } else {
      return new BigDecimal(value.toString()).shortValueExact();
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
        throw new ArithmeticException("integer overflow");
      }
      return value.intValue();
    } else if (value instanceof BigInteger) {
      return ((BigInteger) value).intValueExact();
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).intValueExact();
    } else {
      return new BigDecimal(value.toString()).intValueExact();
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
    } else if (value instanceof BigInteger) {
      return ((BigInteger) value).longValueExact();
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).longValueExact();
    } else {
      return new BigDecimal(value.toString()).longValueExact();
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
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toBigIntegerExact();
    } else {
      return new BigDecimal(value.toString()).toBigIntegerExact();
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
    } else {
      if (Float.isInfinite(value.floatValue()) || Float.isNaN(value.floatValue())) {
        throw new ArithmeticException("floating point overflow");
      }
      if (toBigDecimal(value).compareTo(new BigDecimal(Float.toString(value.floatValue()))) != 0) {
        throw new ArithmeticException("floating point overflow");
      }
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
    } else {
      if (Double.isInfinite(value.doubleValue()) || Double.isNaN(value.doubleValue())) {
        throw new ArithmeticException("floating point overflow");
      }
      if (toBigDecimal(value).compareTo(new BigDecimal(Double.toString(value.doubleValue())))
          != 0) {
        throw new ArithmeticException("floating point overflow");
      }
      return value.doubleValue();
    }
  }

  /**
   * Converts the given number into a BigDecimal.
   *
   * @param value the number to convert; cannot be {@code null}.
   * @return the converted value.
   * @throws NumberFormatException if the number cannot be converted to a {@link BigDecimal}.
   */
  public static BigDecimal toBigDecimal(@NotNull Number value) throws NumberFormatException {
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
      return new BigDecimal(value.toString());
    }
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
   * @throws DateTimeException if the temporal cannot be converted.
   * @throws IllegalArgumentException if target class is unknown.
   */
  @SuppressWarnings("unchecked")
  public static <T extends TemporalAccessor> T convertTemporal(
      TemporalAccessor value,
      @NotNull Class<? extends T> targetClass,
      @NotNull ZoneId timeZone,
      @NotNull LocalDate epoch)
      throws DateTimeException, IllegalArgumentException {
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
    throw new IllegalArgumentException(
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
        instant = instantCodec.convertFrom(s);
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
}
