/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs;

import static com.datastax.dsbulk.commons.codecs.util.OverflowStrategy.REJECT;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.RANDOM;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class ExtendedCodecRegistryBuilder {

  private static final ArrayList<String> DEFAULT_BOOLEAN_STRINGS =
      Lists.newArrayList("1:0", "Y:N", "T:F", "YES:NO", "TRUE:FALSE");

  private Locale locale = Locale.US;
  private ZoneId timeZone = ZoneOffset.UTC;
  private boolean formatNumbers = false;
  private RoundingMode roundingMode = UNNECESSARY;
  private OverflowStrategy overflowStrategy = REJECT;
  private TimeUnit timeUnit = MILLISECONDS;
  private ZonedDateTime epoch = Instant.EPOCH.atZone(timeZone);
  private TimeUUIDGenerator uuidGenerator = RANDOM;
  private String numberFormat = "#,###.##";
  private String timestampFormat = "CQL_TIMESTAMP";
  private String dateFormat = "ISO_LOCAL_DATE";
  private String timeFormat = "ISO_LOCAL_TIME";
  private List<String> nullStrings = new ArrayList<>();
  private Map<String, Boolean> booleanInputWords =
      CodecUtils.getBooleanInputWords(DEFAULT_BOOLEAN_STRINGS);
  private Map<Boolean, String> booleanOutputWords =
      CodecUtils.getBooleanOutputWords(DEFAULT_BOOLEAN_STRINGS);
  private List<BigDecimal> booleanNumbers = Lists.newArrayList(BigDecimal.ONE, BigDecimal.ZERO);
  private ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private boolean allowExtraFields = false;
  private boolean allowMissingFields = false;

  /**
   * Sets the locale to use for locale-sensitive conversions. The default is {@link Locale#US}.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withLocale(@NonNull Locale locale) {
    this.locale = Objects.requireNonNull(locale);
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
  public ExtendedCodecRegistryBuilder withTimeZone(@NonNull ZoneId timeZone) {
    this.timeZone = Objects.requireNonNull(timeZone);
    return this;
  }

  /**
   * Whether or not to use the {@linkplain #withNumberFormat(String) numeric pattern} to format
   * numeric output. The default is {@code false}.
   *
   * <p>When set to {@code true}, {@linkplain #withNumberFormat(String) numeric pattern} will be
   * applied when formatting. This allows for nicely-formatted output, but may result in {@linkplain
   * #withRoundingMode(RoundingMode) rounding} or alteration of the original decimal's scale. When
   * set to {@code false}, numbers will be stringified using the {@code toString()} method, and will
   * never result in rounding or scale alteration. Only applicable when unloading, and only if the
   * connector in use requires stringification, because the connector, such as the CSV connector,
   * does not handle raw numeric data; ignored otherwise.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withFormatNumbers(boolean formatNumbers) {
    this.formatNumbers = formatNumbers;
    return this;
  }

  /**
   * The rounding strategy to use for conversions from CQL numeric types to {@code String}. The
   * default is {@link RoundingMode#UNNECESSARY}.
   *
   * <p>Only applicable when unloading, if {@link #withFormatNumbers(boolean)} is true and if the
   * connector in use requires stringification, because the connector, such as the CSV connector,
   * does not handle raw numeric data; ignored otherwise.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withRoundingMode(@NonNull RoundingMode roundingMode) {
    this.roundingMode = Objects.requireNonNull(roundingMode);
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
  public ExtendedCodecRegistryBuilder withOverflowStrategy(
      @NonNull OverflowStrategy overflowStrategy) {
    this.overflowStrategy = Objects.requireNonNull(overflowStrategy);
    return this;
  }

  /**
   * This setting applies only to CQL {@code timestamp} columns, and {@code USING TIMESTAMP} clauses
   * in queries. If the input is a string containing only digits that cannot be parsed using the
   * {@linkplain #withTimestampFormat(String) timestamp format}, the specified time unit is applied
   * to the parsed value.
   *
   * <p>The default is {@link TimeUnit#MILLISECONDS}.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withTimeUnit(@NonNull TimeUnit unit) {
    this.timeUnit = Objects.requireNonNull(unit);
    return this;
  }

  /**
   * This setting applies only to CQL {@code timestamp} columns, and {@code USING TIMESTAMP} clauses
   * in queries. If the input is a string containing only digits that cannot be parsed using the
   * {@linkplain #withTimestampFormat(String) timestamp format}, the specified epoch determines the
   * relative point in time used with the parsed value.
   *
   * <p>The default is {@link Instant#EPOCH} at {@link ZoneOffset#UTC}.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withEpoch(@NonNull ZonedDateTime epoch) {
    this.epoch = Objects.requireNonNull(epoch);
    return this;
  }

  /**
   * Strategy to use when generating time-based (version 1) UUIDs from timestamps. The default is
   * {@link TimeUUIDGenerator#RANDOM}.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withUuidGenerator(@NonNull TimeUUIDGenerator uuidGenerator) {
    this.uuidGenerator = Objects.requireNonNull(uuidGenerator);
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
  public ExtendedCodecRegistryBuilder withNumberFormat(@NonNull String numberFormat) {
    this.numberFormat = Objects.requireNonNull(numberFormat);
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
   * *
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withTimestampFormat(@NonNull String timestampFormat) {
    this.timestampFormat = Objects.requireNonNull(timestampFormat);
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
  public ExtendedCodecRegistryBuilder withDateFormat(@NonNull String dateFormat) {
    this.dateFormat = Objects.requireNonNull(dateFormat);
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
  public ExtendedCodecRegistryBuilder withTimeFormat(@NonNull String timeFormat) {
    this.timeFormat = Objects.requireNonNull(timeFormat);
    return this;
  }

  /**
   * Specify how {@code true} and {@code false} representations can be used by dsbulk.
   *
   * <p>Each element of the list must be of the form {@code true_value:false_value},
   * case-insensitive. The default list is {@code "1:0", "Y:N", "T:F", "YES:NO", "TRUE:FALSE"}.
   *
   * <p>For loading, all representations are honored: when a record field value exactly matches one
   * of the specified strings, the value is replaced with {@code true} or {@code false} before
   * writing to DSE.
   *
   * <p>For unloading, this setting is only applicable for string-based connectors, such as the CSV
   * connector: the first representation will be used to format booleans before they are written
   * out, and all others are ignored.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withBooleanStrings(@NonNull List<String> booleanStrings) {
    List<String> list = Objects.requireNonNull(booleanStrings);
    booleanInputWords = CodecUtils.getBooleanInputWords(list);
    booleanOutputWords = CodecUtils.getBooleanOutputWords(list);
    return this;
  }

  /**
   * Specify how {@code true} and {@code false} representations can be used by dsbulk.
   *
   * <p>Each element of the list must be of the form {@code true_value:false_value},
   * case-insensitive. The default list is {@code "1:0", "Y:N", "T:F", "YES:NO", "TRUE:FALSE"}.
   *
   * <p>For loading, all representations are honored: when a record field value exactly matches one
   * of the specified strings, the value is replaced with {@code true} or {@code false} before
   * writing to DSE.
   *
   * <p>For unloading, this setting is only applicable for string-based connectors, such as the CSV
   * connector: the first representation will be used to format booleans before they are written
   * out, and all others are ignored.
   *
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("unused")
  public ExtendedCodecRegistryBuilder withBooleanStrings(@NonNull String... booleanStrings) {
    return withBooleanStrings(Arrays.asList(booleanStrings));
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
  public ExtendedCodecRegistryBuilder withNullStrings(@NonNull List<String> nullStrings) {
    this.nullStrings = Objects.requireNonNull(nullStrings);
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
  public ExtendedCodecRegistryBuilder withNullStrings(@NonNull String... nullStrings) {
    this.nullStrings = Arrays.asList(Objects.requireNonNull(nullStrings));
    return this;
  }

  /**
   * Sets how numbers are mapped to boolean values. The default is {@link BigDecimal#ONE} for {@code
   * true} and{@link BigDecimal#ZERO} for {@code false}.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withBooleanNumbers(
      @NonNull BigDecimal trueNumber, BigDecimal falseNumber) {
    this.booleanNumbers = Lists.newArrayList(trueNumber, falseNumber);
    return this;
  }

  /**
   * The object mapper to use to convert to/from Json.
   *
   * @return this builder (for method chaining).
   */
  public ExtendedCodecRegistryBuilder withObjectMapper(@NonNull ObjectMapper objectMapper) {
    this.objectMapper = Objects.requireNonNull(objectMapper);
    return this;
  }

  public ExtendedCodecRegistryBuilder allowExtraFields(boolean allowExtraFields) {
    this.allowExtraFields = allowExtraFields;
    return this;
  }

  public ExtendedCodecRegistryBuilder allowMissingFields(boolean allowMissingFields) {
    this.allowMissingFields = allowMissingFields;
    return this;
  }

  @NonNull
  public ExtendedCodecRegistry build() {
    FastThreadLocal<NumberFormat> numberFormat =
        CodecUtils.getNumberFormatThreadLocal(
            this.numberFormat, locale, roundingMode, formatNumbers);
    TemporalFormat dateFormat =
        CodecUtils.getTemporalFormat(
            this.dateFormat, timeZone, locale, timeUnit, epoch, numberFormat, false);
    TemporalFormat timeFormat =
        CodecUtils.getTemporalFormat(
            this.timeFormat, timeZone, locale, timeUnit, epoch, numberFormat, false);
    TemporalFormat timestampFormat =
        CodecUtils.getTemporalFormat(
            this.timestampFormat, timeZone, locale, timeUnit, epoch, numberFormat, true);
    return new ExtendedCodecRegistry(
        nullStrings,
        booleanInputWords,
        booleanOutputWords,
        booleanNumbers,
        numberFormat,
        overflowStrategy,
        roundingMode,
        dateFormat,
        timeFormat,
        timestampFormat,
        timeZone,
        timeUnit,
        epoch,
        uuidGenerator,
        objectMapper,
        allowExtraFields,
        allowMissingFields);
  }
}
