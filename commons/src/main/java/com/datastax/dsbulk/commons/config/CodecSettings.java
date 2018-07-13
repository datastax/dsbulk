/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.config;

import com.datastax.dsbulk.commons.codecs.CustomCodecFactory;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.util.CqlTemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.ExactNumberFormat;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.SimpleTemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.dsbulk.commons.codecs.util.ToStringNumberFormat;
import com.datastax.dsbulk.commons.codecs.util.ZonedTemporalFormat;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CodecSettings {

  /**
   * A {@link JsonNodeFactory} that preserves {@link BigDecimal} scales, used to generate Json
   * nodes.
   */
  public static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  private static final String CQL_TIMESTAMP = "CQL_TIMESTAMP";
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
  private TemporalFormat localDateFormat;
  private TemporalFormat localTimeFormat;
  private TemporalFormat timestampFormat;
  private ObjectMapper objectMapper;
  private ZoneId timeZone;
  private TimeUnit timeUnit;
  private ZonedDateTime epoch;
  private TimeUUIDGenerator generator;

  public CodecSettings(LoaderConfig config) {
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
      timeZone = ZoneId.of(config.getString(TIME_ZONE));
      timeUnit = config.getEnum(TimeUnit.class, NUMERIC_TIMESTAMP_UNIT);
      String epochStr = config.getString(NUMERIC_TIMESTAMP_EPOCH);
      try {
        epoch = ZonedDateTime.parse(epochStr);
      } catch (Exception e) {
        throw new BulkConfigurationException(
            String.format(
                "Expecting codec.%s to be in ISO_ZONED_DATE_TIME format but got '%s'",
                NUMERIC_TIMESTAMP_EPOCH, epochStr));
      }
      localDateFormat = getTemporalFormat(config.getString(DATE), null, locale);
      localTimeFormat = getTemporalFormat(config.getString(TIME), null, locale);
      timestampFormat = getTemporalFormat(config.getString(TIMESTAMP), timeZone, locale);

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

  public ExtendedCodecRegistry createCodecRegistry(CodecRegistry codecRegistry) {
    return createCodecRegistry(codecRegistry, null);
  }

  @SuppressWarnings("WeakerAccess")
  public ExtendedCodecRegistry createCodecRegistry(
      CodecRegistry codecRegistry, CustomCodecFactory customCodecFactory) {
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
        timeZone,
        timeUnit,
        epoch,
        generator,
        objectMapper,
        customCodecFactory);
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

  /**
   * The object mapper to use for converting Json nodes to and from Java types in Json codecs.
   *
   * <p>This is not the object mapper used by the Json connector to read and write Json files.
   *
   * @return The object mapper to use for converting Json nodes to and from Java types in Json
   *     codecs.
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
