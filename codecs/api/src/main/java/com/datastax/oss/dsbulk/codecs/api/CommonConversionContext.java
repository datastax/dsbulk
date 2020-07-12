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
package com.datastax.oss.dsbulk.codecs.api;

import static java.math.RoundingMode.UNNECESSARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.util.Base64BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.util.BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.api.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.api.util.TemporalFormat;
import com.datastax.oss.dsbulk.codecs.api.util.TimeUUIDGenerator;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A collection of most common conversion context attributes used in DSBulk and other projects.
 * These settings can be reused and extended by modules depending on the dsbulk-codec-api module.
 */
public class CommonConversionContext extends ConversionContext {

  private static final Map<String, Boolean> DEFAULT_BOOLEAN_INPUT_WORDS =
      ImmutableMap.<String, Boolean>builder()
          .put("1", true)
          .put("0", false)
          .put("y", true)
          .put("n", false)
          .put("t", true)
          .put("f", false)
          .put("yes", true)
          .put("no", false)
          .put("true", true)
          .put("false", false)
          .build();

  private static final Map<Boolean, String> DEFAULT_BOOLEAN_OUTPUT_WORDS =
      ImmutableMap.of(true, "1", false, "0");

  public static final String LOCALE = "LOCALE";
  public static final String FORMAT_NUMBERS = "FORMAT_NUMBERS";
  public static final String NULL_STRINGS = "NULL_STRINGS";
  public static final String NUMERIC_PATTERN = "NUMERIC_PATTERN";
  public static final String NUMBER_FORMAT = "NUMBER_FORMAT";
  public static final String OVERFLOW_STRATEGY = "OVERFLOW_STRATEGY";
  public static final String ROUNDING_MODE = "ROUNDING_MODE";
  public static final String TIMESTAMP_PATTERN = "TIMESTAMP_PATTERN";
  public static final String DATE_PATTERN = "DATE_PATTERN";
  public static final String TIME_PATTERN = "TIME_PATTERN";
  public static final String TIMESTAMP_FORMAT = "TIMESTAMP_FORMAT";
  public static final String LOCAL_DATE_FORMAT = "LOCAL_DATE_FORMAT";
  public static final String LOCAL_TIME_FORMAT = "LOCAL_TIME_FORMAT";
  public static final String TIME_ZONE = "TIME_ZONE";
  public static final String TIME_UNIT = "TIME_UNIT";
  public static final String EPOCH = "EPOCH";
  public static final String BOOLEAN_INPUT_WORDS = "BOOLEAN_INPUT_WORDS";
  public static final String BOOLEAN_OUTPUT_WORDS = "BOOLEAN_OUTPUT_WORDS";
  public static final String BOOLEAN_NUMBERS = "BOOLEAN_NUMBERS";
  public static final String TIME_UUID_GENERATOR = "TIME_UUID_GENERATOR";
  public static final String BINARY_FORMAT = "BINARY_FORMAT";
  public static final String ALLOW_EXTRA_FIELDS = "ALLOW_EXTRA_FIELDS";
  public static final String ALLOW_MISSING_FIELDS = "ALLOW_MISSING_FIELDS";

  public CommonConversionContext() {
    addAttribute(LOCALE, Locale.US);
    addAttribute(TIME_ZONE, ZoneOffset.UTC);
    addAttribute(FORMAT_NUMBERS, false);
    addAttribute(ROUNDING_MODE, UNNECESSARY);
    addAttribute(OVERFLOW_STRATEGY, OverflowStrategy.REJECT);
    addAttribute(TIME_UNIT, MILLISECONDS);
    addAttribute(EPOCH, Instant.EPOCH.atZone(ZoneOffset.UTC));
    addAttribute(TIME_UUID_GENERATOR, TimeUUIDGenerator.RANDOM);
    addAttribute(NUMERIC_PATTERN, "#,###.##");
    addAttribute(TIMESTAMP_PATTERN, "CQL_TIMESTAMP");
    addAttribute(DATE_PATTERN, "ISO_LOCAL_DATE");
    addAttribute(TIME_PATTERN, "ISO_LOCAL_TIME");
    addAttribute(NULL_STRINGS, new ArrayList<>());
    addAttribute(BOOLEAN_INPUT_WORDS, DEFAULT_BOOLEAN_INPUT_WORDS);
    addAttribute(BOOLEAN_OUTPUT_WORDS, DEFAULT_BOOLEAN_OUTPUT_WORDS);
    addAttribute(BOOLEAN_NUMBERS, Lists.newArrayList(BigDecimal.ONE, BigDecimal.ZERO));
    addAttribute(ALLOW_EXTRA_FIELDS, false);
    addAttribute(ALLOW_MISSING_FIELDS, false);
    addAttribute(BINARY_FORMAT, Base64BinaryFormat.INSTANCE);
    rebuildFormats();
  }

  /**
   * Sets the locale to use for locale-sensitive conversions. The default is {@link Locale#US}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setLocale(@NonNull Locale locale) {
    addAttribute(LOCALE, Objects.requireNonNull(locale));
    rebuildFormats();
    return this;
  }

  /**
   * Sets the time zone to use for temporal conversions. The default is {@link ZoneOffset#UTC}.
   *
   * <p>When loading, the time zone will be used to obtain a timestamp from inputs that do not
   * convey any explicit time zone information. When unloading, the time zone will be used to format
   * all timestamps.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setTimeZone(@NonNull ZoneId timeZone) {
    addAttribute(TIME_ZONE, Objects.requireNonNull(timeZone));
    rebuildFormats();
    return this;
  }

  /**
   * Whether or not to use the {@linkplain #setNumberFormat(String) numeric pattern} to format
   * numeric output. The default is {@code false}.
   *
   * <p>When set to {@code true}, {@linkplain #setNumberFormat(String) numeric pattern} will be
   * applied when formatting. This allows for nicely-formatted output, but may result in {@linkplain
   * #setRoundingMode(RoundingMode) rounding} or alteration of the original decimal's scale. When
   * set to {@code false}, numbers will be stringified using the {@code toString()} method, and will
   * never result in rounding or scale alteration. Only applicable when unloading, and only if the
   * connector in use requires stringification, because the connector, such as the CSV connector,
   * does not handle raw numeric data; ignored otherwise.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setFormatNumbers(boolean formatNumbers) {
    addAttribute(FORMAT_NUMBERS, formatNumbers);
    rebuildFormats();
    return this;
  }

  /**
   * The rounding strategy to use for conversions from CQL numeric types to {@code String}. The
   * default is {@link RoundingMode#UNNECESSARY}.
   *
   * <p>Only applicable when unloading, if {@link #setFormatNumbers(boolean)} is true and if the
   * connector in use requires stringification, because the connector, such as the CSV connector,
   * does not handle raw numeric data; ignored otherwise.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setRoundingMode(@NonNull RoundingMode roundingMode) {
    addAttribute(ROUNDING_MODE, roundingMode);
    rebuildFormats();
    return this;
  }

  /**
   * The overflow strategy to apply. See {@link OverflowStrategy} javadocs for overflow definitions.
   * The default is {@link OverflowStrategy#REJECT}.
   *
   * <p>Only applicable for loading, when parsing numeric inputs; it does not apply for unloading,
   * since formatting never results in overflow.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setOverflowStrategy(@NonNull OverflowStrategy overflowStrategy) {
    addAttribute(OVERFLOW_STRATEGY, overflowStrategy);
    rebuildFormats();
    return this;
  }

  /**
   * This setting applies only to CQL {@code timestamp} columns, and {@code USING TIMESTAMP} clauses
   * in queries. If the input is a string containing only digits that cannot be parsed using the
   * {@linkplain #setTimestampFormat(String) timestamp format}, the specified time unit is applied
   * to the parsed value.
   *
   * <p>The default is {@link TimeUnit#MILLISECONDS}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setTimeUnit(@NonNull TimeUnit unit) {
    addAttribute(TIME_UNIT, unit);
    rebuildFormats();
    return this;
  }

  /**
   * This setting applies only to CQL {@code timestamp} columns, and {@code USING TIMESTAMP} clauses
   * in queries. If the input is a string containing only digits that cannot be parsed using the
   * {@linkplain #setTimestampFormat(String) timestamp format}, the specified epoch determines the
   * relative point in time used with the parsed value.
   *
   * <p>The default is {@link Instant#EPOCH} at {@link ZoneOffset#UTC}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setEpoch(@NonNull ZonedDateTime epoch) {
    addAttribute(EPOCH, epoch);
    rebuildFormats();
    return this;
  }

  /**
   * Strategy to use when generating time-based (version 1) UUIDs from timestamps. The default is
   * {@link TimeUUIDGenerator#RANDOM}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setTimeUUIDGenerator(@NonNull TimeUUIDGenerator uuidGenerator) {
    addAttribute(TIME_UUID_GENERATOR, uuidGenerator);
    return this;
  }

  /**
   * The numeric pattern to use for conversions between {@code String} and CQL numeric types. The
   * default is {@code #,###.##}.
   *
   * <p>See {@link java.text.DecimalFormat} javadocs for details about the pattern syntax to use.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setNumberFormat(@NonNull String numberFormat) {
    addAttribute(NUMERIC_PATTERN, numberFormat);
    rebuildFormats();
    return this;
  }

  /**
   * The temporal pattern to use for {@code String} to CQL {@code timestamp} conversion. The default
   * is {@code CQL_TIMESTAMP}.
   *
   * <p>Valid choices:
   *
   * <ul>
   *   <li>A date-time pattern such as {@code yyyy-MM-dd HH:mm:ss}
   *   <li>A pre-defined formatter such as {@code ISO_ZONED_DATE_TIME} or {@code ISO_INSTANT}. Any
   *       public static field in {@link java.time.format.DateTimeFormatter} can be used.
   *   <li>The special formatter {@code CQL_TIMESTAMP}, which is a special parser that accepts all
   *       valid CQL literal formats for the {@code timestamp} type.When parsing, this format
   *       recognizes all CQL temporal literals; if the input is a local date or date/time, the
   *       timestamp is resolved using the time zone specified under {@code timeZone}. When
   *       formatting, this format uses the {@code ISO_OFFSET_DATE_TIME} pattern, which is compliant
   *       with both CQL and ISO-8601.
   * </ul>
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setTimestampFormat(@NonNull String timestampFormat) {
    addAttribute(TIMESTAMP_PATTERN, timestampFormat);
    rebuildFormats();
    return this;
  }

  /**
   * The temporal pattern to use for {@code String} to CQL {@code date} conversion. The default is
   * {@code yyyy-MM-dd}.
   *
   * <p>Valid choices:
   *
   * <ul>
   *   <li>A date-time pattern such as {@code yyyy-MM-dd}.
   *   <li>A pre-defined formatter such as {@code ISO_LOCAL_DATE}. Any public static field in {@link
   *       java.time.format.DateTimeFormatter} can be used.
   * </ul>
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setDateFormat(@NonNull String dateFormat) {
    addAttribute(DATE_PATTERN, dateFormat);
    rebuildFormats();
    return this;
  }

  /**
   * The temporal pattern to use for {@code String} to CQL {@code time} conversion.The default is
   * {@code HH:mm:ss}.
   *
   * <p>Valid choices:
   *
   * <ul>
   *   <li>A date-time pattern, such as {@code HH:mm:ss}.
   *   <li>A pre-defined formatter, such as {@code ISO_LOCAL_TIME}. Any public static field in
   *       {@link java.time.format.DateTimeFormatter} can be used.
   * </ul>
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setTimeFormat(@NonNull String timeFormat) {
    addAttribute(TIME_PATTERN, timeFormat);
    rebuildFormats();
    return this;
  }

  /**
   * Specify how strings map to {@code true} and {@code false}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setBooleanInputWords(@NonNull Map<String, Boolean> inputWords) {
    addAttribute(BOOLEAN_INPUT_WORDS, Objects.requireNonNull(inputWords));
    return this;
  }

  /**
   * Specify how {@code true} and {@code false} map to strings.
   *
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("unused")
  public CommonConversionContext setBooleanOutputWords(@NonNull Map<Boolean, String> outputWords) {
    addAttribute(BOOLEAN_OUTPUT_WORDS, Objects.requireNonNull(outputWords));
    return this;
  }

  /**
   * Comma-separated list of case-sensitive strings that should be mapped to {@code null}.
   *
   * <p>For loading, when a record field value exactly matches one of the specified strings, the
   * value is replaced with {@code null} before writing to DSE.
   *
   * <p>For unloading, this setting is only applicable for string-based connectors, such as the CSV
   * connector: the first string specified will be used to change a row cell containing {@code null}
   * to the specified string when written out.
   *
   * <p>For example, setting this to {@code ["NULL"]} will cause a field containing the word {@code
   * NULL} to be mapped to {@code null} while loading, and a column containing {@code null} to be
   * converted to the word {@code NULL} while unloading.
   *
   * <p>The default value is {@code []} (no strings are mapped to {@code null}). In the default
   * mode, DSBulk behaves as follows: when loading, if the target CQL type is textual (i.e. text,
   * varchar or ascii), the original field value is left untouched; for other types, if the value is
   * an empty string, it is converted to {@code null}; when unloading, all {@code null} values are
   * converted to an empty string.
   *
   * <p>Note that, regardless of this setting, DSBulk will always convert empty strings to {@code
   * null} if the target CQL type is not textual (i.e. not text, varchar or ascii).
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setNullStrings(@NonNull List<String> nullStrings) {
    addAttribute(NULL_STRINGS, nullStrings);
    rebuildFormats();
    return this;
  }

  /**
   * Comma-separated list of case-sensitive strings that should be mapped to {@code null}.
   *
   * <p>For loading, when a record field value exactly matches one of the specified strings, the
   * value is replaced with {@code null} before writing to DSE.
   *
   * <p>For unloading, this setting is only applicable for string-based connectors, such as the CSV
   * connector: the first string specified will be used to change a row cell containing {@code null}
   * to the specified string when written out.
   *
   * <p>For example, setting this to {@code ["NULL"]} will cause a field containing the word {@code
   * NULL} to be mapped to {@code null} while loading, and a column containing {@code null} to be
   * converted to the word {@code NULL} while unloading.
   *
   * <p>The default value is {@code []} (no strings are mapped to {@code null}). In the default
   * mode, DSBulk behaves as follows: when loading, if the target CQL type is textual (i.e. text,
   * varchar or ascii), the original field value is left untouched; for other types, if the value is
   * an empty string, it is converted to {@code null}; when unloading, all {@code null} values are
   * converted to an empty string.
   *
   * <p>Note that, regardless of this setting, DSBulk will always convert empty strings to {@code
   * null} if the target CQL type is not textual (i.e. not text, varchar or ascii).
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setNullStrings(@NonNull String... nullStrings) {
    return setNullStrings(Arrays.asList(Objects.requireNonNull(nullStrings)));
  }

  /**
   * Sets how numbers are mapped to boolean values. The default is {@link BigDecimal#ONE} for {@code
   * true} and{@link BigDecimal#ZERO} for {@code false}.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setBooleanNumbers(
      @NonNull BigDecimal trueNumber, BigDecimal falseNumber) {
    addAttribute(BOOLEAN_NUMBERS, Lists.newArrayList(trueNumber, falseNumber));
    return this;
  }

  public CommonConversionContext setAllowExtraFields(boolean allowExtraFields) {
    addAttribute(ALLOW_EXTRA_FIELDS, allowExtraFields);
    return this;
  }

  public CommonConversionContext setAllowMissingFields(boolean allowMissingFields) {
    addAttribute(ALLOW_MISSING_FIELDS, allowMissingFields);
    return this;
  }

  /**
   * The binary format to use for conversions between {@code String} and CQL blob.
   *
   * @return this builder (for method chaining).
   */
  public CommonConversionContext setBinaryFormat(@NonNull BinaryFormat binaryFormat) {
    addAttribute(BINARY_FORMAT, binaryFormat);
    return this;
  }

  private void rebuildFormats() {
    String numericPattern = getAttribute(NUMERIC_PATTERN);
    String timestampPattern = getAttribute(TIMESTAMP_PATTERN);
    String datePattern = getAttribute(DATE_PATTERN);
    String timePattern = getAttribute(TIME_PATTERN);
    Locale locale = getAttribute(LOCALE);
    ZoneId timeZone = getAttribute(TIME_ZONE);
    TimeUnit timeUnit = getAttribute(TIME_UNIT);
    ZonedDateTime epoch = getAttribute(EPOCH);
    RoundingMode roundingMode = getAttribute(ROUNDING_MODE);
    boolean formatNumbers = getAttribute(FORMAT_NUMBERS);
    FastThreadLocal<NumberFormat> numberFormat =
        CodecUtils.getNumberFormatThreadLocal(numericPattern, locale, roundingMode, formatNumbers);
    TemporalFormat dateFormat =
        CodecUtils.getTemporalFormat(
            datePattern, timeZone, locale, timeUnit, epoch, numberFormat, false);
    TemporalFormat timeFormat =
        CodecUtils.getTemporalFormat(
            timePattern, timeZone, locale, timeUnit, epoch, numberFormat, false);
    TemporalFormat timestampFormat =
        CodecUtils.getTemporalFormat(
            timestampPattern, timeZone, locale, timeUnit, epoch, numberFormat, true);
    addAttribute(NUMBER_FORMAT, numberFormat);
    addAttribute(LOCAL_DATE_FORMAT, dateFormat);
    addAttribute(LOCAL_TIME_FORMAT, timeFormat);
    addAttribute(TIMESTAMP_FORMAT, timestampFormat);
  }
}
