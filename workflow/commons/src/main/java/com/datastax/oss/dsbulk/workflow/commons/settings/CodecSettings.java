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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.util.Base64BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.util.BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.api.util.HexBinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.api.util.TimeUUIDGenerator;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CodecSettings {

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
  private static final String BINARY = "binary";

  private final Config config;

  private Locale locale;
  private ImmutableList<String> nullStrings;
  private List<BigDecimal> booleanNumbers;
  private boolean formatNumbers;
  private String numberFormat;
  private RoundingMode roundingMode;
  private OverflowStrategy overflowStrategy;
  private String dateFormat;
  private String timeFormat;
  private String timestampFormat;
  private ObjectMapper objectMapper;
  private ZoneId timeZone;
  private TimeUnit timeUnit;
  private ZonedDateTime epoch;
  private TimeUUIDGenerator generator;
  private Map<String, Boolean> booleanInputWords;
  private Map<Boolean, String> booleanOutputWords;
  private BinaryFormat binaryFormat;

  public CodecSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {

      locale = CodecUtils.parseLocale(config.getString(LOCALE));

      // strings
      nullStrings = ImmutableList.copyOf(config.getStringList(NULL_STRINGS));

      // numeric
      roundingMode = config.getEnum(RoundingMode.class, ROUNDING_STRATEGY);
      overflowStrategy = config.getEnum(OverflowStrategy.class, OVERFLOW_STRATEGY);
      formatNumbers = config.getBoolean(FORMAT_NUMERIC_OUTPUT);
      numberFormat = config.getString(NUMBER);

      // temporal
      timeZone = ZoneId.of(config.getString(TIME_ZONE));
      timeUnit = config.getEnum(TimeUnit.class, NUMERIC_TIMESTAMP_UNIT);
      String epochStr = config.getString(NUMERIC_TIMESTAMP_EPOCH);
      try {
        epoch = ZonedDateTime.parse(epochStr);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for dsbulk.codec.%s, expecting temporal in ISO_ZONED_DATE_TIME format, got '%s'",
                NUMERIC_TIMESTAMP_EPOCH, epochStr));
      }
      dateFormat = config.getString(DATE);
      timeFormat = config.getString(TIME);
      timestampFormat = config.getString(TIMESTAMP);

      // boolean
      booleanNumbers = getBooleanNumbers(config.getStringList(BOOLEAN_NUMBERS));

      List<String> booleanStrings = config.getStringList(BOOLEAN_STRINGS);
      booleanInputWords = getBooleanInputWords(booleanStrings);
      booleanOutputWords = getBooleanOutputWords(booleanStrings);

      // UUID
      generator = config.getEnum(TimeUUIDGenerator.class, TIME_UUID_GENERATOR);

      // json
      objectMapper = JsonCodecUtils.getObjectMapper();

      // Binary
      binaryFormat = getBinaryFormat();

    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.codec");
    }
  }

  private List<BigDecimal> getBooleanNumbers(List<String> booleanNumbersStr) {
    if (booleanNumbersStr.size() != 2) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value for dsbulk.codec.%s, expecting list with two numbers, got '%s'",
              BOOLEAN_NUMBERS, booleanNumbersStr));
    }
    try {
      return booleanNumbersStr.stream().map(BigDecimal::new).collect(Collectors.toList());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value for dsbulk.codec.%s, expecting list with two numbers, got '%s'",
              BOOLEAN_NUMBERS, booleanNumbersStr));
    }
  }

  private BinaryFormat getBinaryFormat() {
    String binaryFormatStr = config.getString(BINARY);
    switch (binaryFormatStr.toLowerCase()) {
      case "hex":
        return HexBinaryFormat.INSTANCE;
      case "base64":
        return Base64BinaryFormat.INSTANCE;
      default:
        throw new IllegalArgumentException(
            "Invalid value for dsbulk.codec.binary, expecting HEX or BASE64, got "
                + binaryFormatStr);
    }
  }

  public ConvertingCodecFactory createCodecFactory(
      boolean allowExtraFields, boolean allowMissingFields) {
    ConversionContext context =
        new TextConversionContext()
            .setObjectMapper(objectMapper)
            .setLocale(locale)
            .setNullStrings(nullStrings)
            .setBooleanInputWords(booleanInputWords)
            .setBooleanOutputWords(booleanOutputWords)
            .setBooleanNumbers(booleanNumbers.get(0), booleanNumbers.get(1))
            .setNumberFormat(numberFormat)
            .setFormatNumbers(formatNumbers)
            .setOverflowStrategy(overflowStrategy)
            .setRoundingMode(roundingMode)
            .setDateFormat(dateFormat)
            .setTimeFormat(timeFormat)
            .setTimestampFormat(timestampFormat)
            .setTimeZone(timeZone)
            .setTimeUnit(timeUnit)
            .setEpoch(epoch)
            .setTimeUUIDGenerator(generator)
            .setBinaryFormat(binaryFormat)
            .setAllowExtraFields(allowExtraFields)
            .setAllowMissingFields(allowMissingFields);
    return new ConvertingCodecFactory(context);
  }

  public static Map<String, Boolean> getBooleanInputWords(List<String> list) {
    ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
    list.stream()
        .map(str -> new StringTokenizer(str, ":"))
        .forEach(
            tokenizer -> {
              if (tokenizer.countTokens() != 2) {
                throw new IllegalArgumentException(
                    String.format(
                        "Invalid value for dsbulk.codec.booleanStrings, "
                            + "expecting list with at least one true:false pair, got '%s'",
                        list));
              }
              builder.put(tokenizer.nextToken().toLowerCase(), true);
              builder.put(tokenizer.nextToken().toLowerCase(), false);
            });
    return builder.build();
  }

  public static Map<Boolean, String> getBooleanOutputWords(List<String> list) {
    if (list.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value for dsbulk.codec.booleanStrings, "
                  + "expecting list with at least one true:false pair, got '%s'",
              list));
    }
    StringTokenizer tokenizer = new StringTokenizer(list.get(0), ":");
    ImmutableMap.Builder<Boolean, String> builder = ImmutableMap.builder();
    builder.put(true, tokenizer.nextToken().toLowerCase());
    builder.put(false, tokenizer.nextToken().toLowerCase());
    return builder.build();
  }
}
