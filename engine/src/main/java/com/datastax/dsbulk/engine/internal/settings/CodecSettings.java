/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToTemporalCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.ExactNumberFormat;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import com.datastax.dsbulk.engine.internal.codecs.util.ToStringNumberFormat;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;
import io.netty.util.concurrent.FastThreadLocal;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class CodecSettings {

  /** A {@link DateTimeFormatter} that formats and parses most accepted CQL timestamp formats. */
  public static final DateTimeFormatter CQL_DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          // date part
          .optionalStart()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalEnd()

          // date-time separator
          .optionalStart()
          .appendLiteral('T')
          .optionalEnd()

          // time part
          .optionalStart()
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalEnd()

          // time zone part
          .optionalStart()
          // ISO_ZONED_DATE_TIME has appendOffsetId() here, which, when parsing, does not set the
          // zone id
          .appendZoneOrOffsetId()
          .optionalEnd()
          .optionalStart()
          .appendLiteral('[')
          .appendZoneRegionId()
          .appendLiteral(']')
          .optionalEnd()

          // defaults, locale and time zone will be overridden with user-supplied settings
          .parseDefaulting(ChronoField.YEAR, 1970)
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
          .toFormatter(Locale.US)
          .withZone(ZoneOffset.UTC);

  /**
   * A {@link JsonNodeFactory} that preserves {@link BigDecimal} scales, used to generate Json
   * nodes.
   */
  public static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  private static final String CQL_DATE_TIME = "CQL_DATE_TIME";
  private static final String LOCALE = "locale";
  private static final String NULL_STRINGS = "nullStrings";
  private static final String BOOLEAN_STRINGS = "booleanStrings";
  private static final String BOOLEAN_NUMBERS = "booleanNumbers";
  private static final String NUMBER = "number";
  private static final String FORMAT_NUMERIC_OUTPUT = "formatNumbers";
  private static final String ROUNDING_STRATEGY = "roundingStrategy";
  private static final String OVERFLOW_STRATEGY = "overflowStrategy";
  private static final String TIME = "time";
  private static final String TIME_ZONE = "timeZone";
  private static final String DATE = "date";
  private static final String TIMESTAMP = "timestamp";
  private static final String NUMERIC_TIMESTAMP_UNIT = "unit";
  private static final String NUMERIC_TIMESTAMP_EPOCH = "epoch";
  private static final String TIME_UUID_GENERATOR = "uuidStrategy";

  private final LoaderConfig config;

  private ImmutableList<String> nullStrings;
  private Map<String, Boolean> booleanInputWords;
  private Map<Boolean, String> booleanOutputWords;
  private List<BigDecimal> booleanNumbers;
  private FastThreadLocal<NumberFormat> numberFormat;
  private RoundingMode roundingMode;
  private OverflowStrategy overflowStrategy;
  private DateTimeFormatter localDateFormat;
  private DateTimeFormatter localTimeFormat;
  private DateTimeFormatter timestampFormat;
  private ObjectMapper objectMapper;
  private TimeUnit timeUnit;
  private ZonedDateTime epoch;
  private TimeUUIDGenerator generator;

  CodecSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {

      Locale locale = parseLocale(config.getString(LOCALE));

      // strings
      nullStrings = ImmutableList.copyOf(config.getStringList(NULL_STRINGS));

      // numeric
      roundingMode = config.getEnum(RoundingMode.class, ROUNDING_STRATEGY);
      overflowStrategy = config.getEnum(OverflowStrategy.class, OVERFLOW_STRATEGY);
      boolean formatNumbers = config.getBoolean(FORMAT_NUMERIC_OUTPUT);
      numberFormat =
          getNumberFormatThreadLocal(config.getString(NUMBER), locale, roundingMode, formatNumbers);

      // temporal
      ZoneId timeZone = ZoneId.of(config.getString(TIME_ZONE));
      timeUnit = config.getEnum(TimeUnit.class, NUMERIC_TIMESTAMP_UNIT);
      epoch = ZonedDateTime.parse(config.getString(NUMERIC_TIMESTAMP_EPOCH));
      localDateFormat = getDateTimeFormat(config.getString(DATE), timeZone, locale, epoch);
      localTimeFormat = getDateTimeFormat(config.getString(TIME), timeZone, locale, epoch);
      timestampFormat = getDateTimeFormat(config.getString(TIMESTAMP), timeZone, locale, epoch);

      // boolean
      booleanNumbers =
          config
              .getStringList(BOOLEAN_NUMBERS)
              .stream()
              .map(BigDecimal::new)
              .collect(Collectors.toList());
      if (booleanNumbers.size() != 2) {
        throw new BulkConfigurationException(
            "Invalid boolean numbers list, expecting two elements, got " + booleanNumbers);
      }
      List<String> booleanStrings = config.getStringList(BOOLEAN_STRINGS);
      booleanInputWords = getBooleanInputWords(booleanStrings);
      booleanOutputWords = getBooleanOutputWords(booleanStrings);

      // UUID
      generator = config.getEnum(TimeUUIDGenerator.class, TIME_UUID_GENERATOR);

      // json
      objectMapper = getObjectMapper();

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "codec");
    }
  }

  public StringToTemporalCodec<Instant> getTimestampCodec() {
    return new StringToInstantCodec(timestampFormat, numberFormat, timeUnit, epoch, nullStrings);
  }

  public ExtendedCodecRegistry createCodecRegistry(Cluster cluster) {
    CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
    return new ExtendedCodecRegistry(
        codecRegistry,
        nullStrings,
        booleanInputWords,
        booleanOutputWords,
        booleanNumbers,
        numberFormat,
        overflowStrategy,
        roundingMode,
        localDateFormat,
        localTimeFormat,
        timestampFormat,
        timeUnit,
        epoch,
        generator,
        objectMapper);
  }

  private static Locale parseLocale(String s) {
    StringTokenizer tokenizer = new StringTokenizer(s, "_");
    String language = tokenizer.nextToken();
    if (tokenizer.hasMoreTokens()) {
      String country = tokenizer.nextToken();
      if (tokenizer.hasMoreTokens()) {
        String variant = tokenizer.nextToken();
        return new Locale(language, country, variant);
      } else {
        return new Locale(language, country);
      }
    } else {
      return new Locale(language);
    }
  }

  @VisibleForTesting
  public static FastThreadLocal<NumberFormat> getNumberFormatThreadLocal(
      String pattern, Locale locale, RoundingMode roundingMode, boolean formatNumbers) {
    return new FastThreadLocal<NumberFormat>() {
      @Override
      protected NumberFormat initialValue() {
        return getNumberFormat(pattern, locale, roundingMode, formatNumbers);
      }
    };
  }

  @VisibleForTesting
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

  @NotNull
  @VisibleForTesting
  public static DateTimeFormatter getDateTimeFormat(
      String pattern, ZoneId timeZone, Locale locale, ZonedDateTime epoch) {
    DateTimeFormatterBuilder builder =
        new DateTimeFormatterBuilder().parseStrict().parseCaseInsensitive();
    if (pattern.equals(CQL_DATE_TIME)) {
      builder =
          builder
              .append(CQL_DATE_TIME_FORMAT)
              // add defaults for missing fields, which allows the parsing to be lenient when
              // some fields cannot be inferred from the input.
              .parseDefaulting(ChronoField.YEAR, epoch.get(ChronoField.YEAR))
              .parseDefaulting(ChronoField.MONTH_OF_YEAR, epoch.get(ChronoField.MONTH_OF_YEAR))
              .parseDefaulting(ChronoField.DAY_OF_MONTH, epoch.get(ChronoField.DAY_OF_MONTH))
              .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
              .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
              .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
              .parseDefaulting(ChronoField.NANO_OF_SECOND, 0);
    } else {
      try {
        // first, assume it is a predefined format
        Field field = DateTimeFormatter.class.getDeclaredField(pattern);
        DateTimeFormatter formatter = (DateTimeFormatter) field.get(null);
        builder = builder.append(formatter);
      } catch (NoSuchFieldException | IllegalAccessException ignored) {
        // if that fails, assume it's a pattern
        builder = builder.appendPattern(pattern);
      }
    }
    return builder.toFormatter(locale).withZone(timeZone);
  }

  /**
   * The object mapper to use for converting Json nodes to and from Java types in Json codecs.
   *
   * <p>This is not the object mapper used by the Json connector to read and write Json files.
   *
   * @return The object mapper to use for converting Json to collections, UDTs and tuples.
   */
  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setNodeFactory(JSON_NODE_FACTORY);
    // create a somewhat lenient mapper that recognizes a slightly relaxed Json syntax when parsing
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return objectMapper;
  }

  private static Map<String, Boolean> getBooleanInputWords(List<String> list) {
    ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
    list.stream()
        .map(str -> new StringTokenizer(str, ":"))
        .forEach(
            tokenizer -> {
              if (tokenizer.countTokens() != 2) {
                throw new BulkConfigurationException(
                    "Expecting codec.booleanStrings to contain a list of true:false pairs, got "
                        + list);
              }
              builder.put(tokenizer.nextToken().toLowerCase(), true);
              builder.put(tokenizer.nextToken().toLowerCase(), false);
            });
    return builder.build();
  }

  private static Map<Boolean, String> getBooleanOutputWords(List<String> list) {
    StringTokenizer tokenizer = new StringTokenizer(list.get(0), ":");
    ImmutableMap.Builder<Boolean, String> builder = ImmutableMap.builder();
    builder.put(true, tokenizer.nextToken().toLowerCase());
    builder.put(false, tokenizer.nextToken().toLowerCase());
    return builder.build();
  }
}
