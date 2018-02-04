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
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
      @NotNull DecimalFormat numberFormat,
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
      temporal = CodecUtils.parseTemporal(s, temporalFormat);
    } catch (Exception e1) {
      try {
        // 2) try a number, then convert to instant since epoch
        BigDecimal number = CodecUtils.parseNumber(s, numberFormat);
        // bypass overflow strategy, we don't want to alter timestamps
        Long units = CodecUtils.toLongValueExact(number);
        temporal = CodecUtils.numberToInstant(units, timeUnit, epoch);
      } catch (Exception e2) {
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
   * @param numberFormat the {@link DecimalFormat} to use to parse numbers; cannot be {@code null}.
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
      @NotNull DecimalFormat numberFormat,
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
      number = CodecUtils.parseNumber(s, numberFormat);
    } catch (Exception e1) {
      try {
        // 2) try Double.valueOf(s)
        number = Double.valueOf(s);
      } catch (NumberFormatException e2) {
        e2.addSuppressed(e1);
        try {
          // 3) try a temporal, then convert to units since epoch
          TemporalAccessor temporal = CodecUtils.parseTemporal(s, temporalFormat);
          Instant instant = toInstant(temporal, temporalFormat.getZone(), epoch.toLocalDate());
          number = CodecUtils.instantToNumber(instant, timeUnit, epoch.toInstant());
        } catch (Exception e3) {
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
                        CodecUtils.formatNumber(1234.56, numberFormat),
                        CodecUtils.formatTemporal(Instant.now(), temporalFormat)));
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
   * conversion fails, applies the overflow strategy and return the narrowed number.
   *
   * @param <N> The target type.
   * @param value the value to convert.
   * @param targetClass the target class; cannot be {@code null}.
   * @param overflowStrategy the overflow strategy; cannot be {@code null}.
   * @param roundingMode the rounding mode; cannot be {@code null}.
   * @return the narrowed number.
   */
  public static <N extends Number> N narrowNumber(
      Number value,
      Class<? extends N> targetClass,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode) {
    if (value == null) {
      return null;
    }
    try {
      return CodecUtils.convertNumber(value, targetClass);
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
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static Instant numberToInstant(Number n, TimeUnit timeUnit, Instant epoch) {
    if (n == null) {
      return null;
    }
    try {
      Duration duration = Duration.ofNanos(NANOSECONDS.convert(n.longValue(), timeUnit));
      return epoch.plus(duration);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Cannot parse numeric temporal: " + n, e);
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
  public static long instantToNumber(
      @NotNull Instant instant, @NotNull TimeUnit timeUnit, @NotNull Instant epoch) {
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
   * <p>The given formatter must be configured to parse {@link BigDecimal}s, see {@link
   * DecimalFormat#setParseBigDecimal(boolean)}.
   *
   * @param s the string to parse, may be {@code null}.
   * @param decimalFormat the format to use; cannot be {@code null}.
   * @return a {@link BigDecimal}, or {@code null} if the input was {@code null} or empty.
   */
  public static BigDecimal parseNumber(String s, DecimalFormat decimalFormat) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    BigDecimal number = (BigDecimal) decimalFormat.parse(s.trim(), pos);
    if (number == null) {
      throw new IllegalArgumentException(
          "Invalid number format: " + s, new ParseException(s, pos.getErrorIndex()));
    }
    if (pos.getIndex() != s.length()) {
      throw new IllegalArgumentException(
          "Invalid number format: " + s, new ParseException(s, pos.getIndex()));
    }
    return number;
  }

  /**
   * Formats the given number using the given format.
   *
   * @param value the value to format.
   * @param format the format to use; cannot be {@code null}.
   * @return the formatted value.
   */
  public static String formatNumber(Number value, DecimalFormat format) {
    if (value == null) {
      return null;
    }
    // DecimalFormat sometimes applies type narrowing / widening;
    // especially, for decimal values, it widens float -> double and
    // narrows BigDecimal -> double, then formats the resulting double.
    // This poses a problem for floats. The float -> double widening
    // may alter the original value; however, if we first convert float -> BigDecimal,
    // then let DecimalFormat do the BigDecimal -> double narrowing, the result
    // *seems* exact for all floats.
    // To be on the safe side, let's convert everything to BigDecimals before formatting.
    try {
      value = CodecUtils.toBigDecimal(value);
    } catch (NumberFormatException ignored) {
      // happens in rare cases, e.g. with Double.NaN
    }
    return format.format(value);
  }

  /**
   * Parses the given string as an alphanumeric temporal, using the given parser.
   *
   * @param s the string to parse, may be {@code null}.
   * @param dateTimeFormat the format to use; cannot be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws IllegalArgumentException if the string cannot be parsed.
   */
  public static TemporalAccessor parseTemporal(String s, DateTimeFormatter dateTimeFormat) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    TemporalAccessor accessor = dateTimeFormat.parse(s, pos);
    if (pos.getIndex() != s.length()) {
      throw new IllegalArgumentException(
          "Cannot parse temporal: " + s, new ParseException(s, pos.getErrorIndex()));
    }
    return accessor;
  }

  /**
   * Formats the given temporal using the given format.
   *
   * @param value the value to format.
   * @param format the format to use; cannot be {@code null}.
   * @return the formatted value.
   */
  public static String formatTemporal(TemporalAccessor value, DateTimeFormatter format) {
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
   */
  @SuppressWarnings("unchecked")
  public static <N> N convertNumber(Number value, Class<? extends N> targetClass) {
    if (value == null) return null;
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

  public static Byte toByteValueExact(Number value) {
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

  public static Short toShortValueExact(Number value) {
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

  public static Integer toIntValueExact(Number value) {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Byte) {
      return value.intValue();
    } else if (value instanceof Short) {
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

  public static Long toLongValueExact(Number value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Byte) {
      return value.longValue();
    } else if (value instanceof Short) {
      return value.longValue();
    } else if (value instanceof Integer) {
      return value.longValue();
    } else if (value instanceof BigInteger) {
      return ((BigInteger) value).longValueExact();
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).longValueExact();
    } else {
      return new BigDecimal(value.toString()).longValueExact();
    }
  }

  public static BigInteger toBigIntegerExact(Number value) {
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof Byte) {
      return BigInteger.valueOf(value.longValue());
    } else if (value instanceof Short) {
      return BigInteger.valueOf(value.longValue());
    } else if (value instanceof Integer) {
      return BigInteger.valueOf(value.longValue());
    } else if (value instanceof Long) {
      return BigInteger.valueOf(value.longValue());
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toBigIntegerExact();
    } else {
      return new BigDecimal(value.toString()).toBigIntegerExact();
    }
  }

  public static Float toFloatValueExact(Number value) {
    if (value instanceof Float) {
      return (Float) value;
    } else {
      if (Float.isInfinite(value.floatValue()))
        throw new ArithmeticException("floating point overflow");
      if (Float.isNaN(value.floatValue())) throw new ArithmeticException("floating point overflow");
      if (toBigDecimal(value).compareTo(new BigDecimal(Float.toString(value.floatValue()))) != 0) {
        throw new ArithmeticException("floating point overflow");
      }
      return value.floatValue();
    }
  }

  public static Double toDoubleValueExact(Number value) {
    if (value instanceof Double) {
      return (Double) value;
    } else {
      if (Double.isInfinite(value.doubleValue()))
        throw new ArithmeticException("floating point overflow");
      if (Double.isNaN(value.doubleValue()))
        throw new ArithmeticException("floating point overflow");
      if (toBigDecimal(value).compareTo(new BigDecimal(Double.toString(value.doubleValue())))
          != 0) {
        throw new ArithmeticException("floating point overflow");
      }
      return value.doubleValue();
    }
  }

  public static BigDecimal toBigDecimal(Number value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Byte) {
      return BigDecimal.valueOf(value.longValue());
    } else if (value instanceof Short) {
      return BigDecimal.valueOf(value.longValue());
    } else if (value instanceof Integer) {
      return BigDecimal.valueOf(value.longValue());
    } else if (value instanceof Long) {
      return BigDecimal.valueOf(value.longValue());
    } else if (value instanceof BigInteger) {
      return new BigDecimal(((BigInteger) value));
    } else {
      return new BigDecimal(value.toString());
    }
  }

  @SuppressWarnings("unchecked")
  public static <T extends TemporalAccessor> T convertTemporal(
      TemporalAccessor value, Class<? extends T> targetClass, ZoneId timeZone, LocalDate epoch) {
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

  public static ZonedDateTime toZonedDateTime(
      TemporalAccessor value, ZoneId timeZone, LocalDate epoch) {
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

  public static Instant toInstant(TemporalAccessor value, ZoneId timeZone, LocalDate epoch) {
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

  public static LocalDateTime toLocalDateTime(
      TemporalAccessor value, ZoneId timeZone, LocalDate epoch) {
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

  public static LocalDate toLocalDate(TemporalAccessor value, ZoneId timeZone) {
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

  public static LocalTime toLocalTime(TemporalAccessor value, ZoneId timeZone) {
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

  public static UUID parseUUID(
      String s, ConvertingCodec<String, Instant> instantCodec, TimeUUIDGenerator generator) {
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
