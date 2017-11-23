/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

/** */
public class CodecSettings {

  /** A {@link DateTimeFormatter} that formats and parses all accepted CQL timestamp formats. */
  public static final DateTimeFormatter CQL_DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalEnd()
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalEnd()
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .optionalStart()
          .appendZoneId()
          .optionalEnd()
          .toFormatter()
          .withZone(ZoneOffset.UTC);

  private static final String CQL_DATE_TIME = "CQL_DATE_TIME";
  private static final String LOCALE = "locale";
  private static final String BOOLEAN_WORDS = "booleanWords";
  private static final String NUMBER = "number";
  private static final String TIME = "time";
  private static final String TIME_ZONE = "timeZone";
  private static final String DATE = "date";
  private static final String TIMESTAMP = "timestamp";

  private final String localeString;
  private final List<String> booleanWords;
  private final String number;
  private final String time;
  private final String timeZone;
  private final String date;
  private final String timestamp;

  CodecSettings(LoaderConfig config) {
    try {
      localeString = config.getString(LOCALE);
      booleanWords = config.getStringList(BOOLEAN_WORDS);
      number = config.getString(NUMBER);
      timeZone = config.getString(TIME_ZONE);
      date = config.getString(DATE);
      time = config.getString(TIME);
      timestamp = config.getString(TIMESTAMP);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "codec");
    }
  }

  public ExtendedCodecRegistry createCodecRegistry(Cluster cluster) {
    Locale locale = parseLocale(localeString);
    Map<String, Boolean> booleanInputs = getBooleanInputs(booleanWords);
    Map<Boolean, String> booleanOutputs = getBooleanOutputs(booleanWords);
    ThreadLocal<DecimalFormat> numberFormat = getNumberFormat(locale, number);
    DateTimeFormatter localDateFormat = getDateFormat(date, timeZone, locale);
    DateTimeFormatter localTimeFormat = getDateFormat(time, timeZone, locale);
    DateTimeFormatter timestampFormat = getDateFormat(timestamp, timeZone, locale);
    ObjectMapper objectMapper = getObjectMapper();
    CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
    return new ExtendedCodecRegistry(
        codecRegistry,
        booleanInputs,
        booleanOutputs,
        numberFormat,
        localDateFormat,
        localTimeFormat,
        timestampFormat,
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

  private static ThreadLocal<DecimalFormat> getNumberFormat(Locale locale, String decimalPattern) {
    return ThreadLocal.withInitial(
        () -> new DecimalFormat(decimalPattern, DecimalFormatSymbols.getInstance(locale)));
  }

  private static DateTimeFormatter getDateFormat(
      String constantOrPattern, String timeZone, Locale locale) {
    if (constantOrPattern.equalsIgnoreCase(CQL_DATE_TIME)) return CQL_DATE_TIME_FORMAT;
    try {
      Field field = DateTimeFormatter.class.getDeclaredField(constantOrPattern);
      DateTimeFormatter formatter = (DateTimeFormatter) field.get(null);
      return new DateTimeFormatterBuilder()
          .append(formatter)
          .parseStrict()
          .toFormatter(locale)
          .withZone(ZoneId.of(timeZone));
    } catch (NoSuchFieldException | IllegalAccessException ignored) {
    }
    return new DateTimeFormatterBuilder()
        .appendPattern(constantOrPattern)
        .parseStrict()
        .toFormatter(locale)
        .withZone(ZoneId.of(timeZone));
  }

  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    // create a somewhat lenient mapper that recognizes a slightly relaxed Json syntax when parsing
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return objectMapper;
  }

  private static Map<String, Boolean> getBooleanInputs(List<String> list) {
    ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
    list.stream()
        .map(str -> new StringTokenizer(str, ":"))
        .forEach(
            tokenizer -> {
              builder.put(tokenizer.nextToken().toLowerCase(), true);
              builder.put(tokenizer.nextToken().toLowerCase(), false);
            });
    return builder.build();
  }

  private static Map<Boolean, String> getBooleanOutputs(List<String> list) {
    StringTokenizer tokenizer = new StringTokenizer(list.get(0), ":");
    ImmutableMap.Builder<Boolean, String> builder = ImmutableMap.builder();
    builder.put(true, tokenizer.nextToken().toLowerCase());
    builder.put(false, tokenizer.nextToken().toLowerCase());
    return builder.build();
  }
}
