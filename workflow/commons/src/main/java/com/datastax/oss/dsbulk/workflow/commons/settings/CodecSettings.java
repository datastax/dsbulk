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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
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
  private List<String> booleanStrings;

  @VisibleForTesting
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
                "Expecting codec.%s to be in ISO_ZONED_DATE_TIME format but got '%s'",
                NUMERIC_TIMESTAMP_EPOCH, epochStr));
      }
      dateFormat = config.getString(DATE);
      timeFormat = config.getString(TIME);
      timestampFormat = config.getString(TIMESTAMP);

      // boolean
      booleanNumbers =
          config.getStringList(BOOLEAN_NUMBERS).stream()
              .map(BigDecimal::new)
              .collect(Collectors.toList());
      if (booleanNumbers.size() != 2) {
        throw new IllegalArgumentException(
            "Invalid boolean numbers list, expecting two elements, got " + booleanNumbers);
      }
      booleanStrings = config.getStringList(BOOLEAN_STRINGS);

      // UUID
      generator = config.getEnum(TimeUUIDGenerator.class, TIME_UUID_GENERATOR);

      // json
      objectMapper = JsonCodecUtils.getObjectMapper();

    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.codec");
    }
  }

  public ConvertingCodecFactory createCodecFactory(
      boolean allowExtraFields, boolean allowMissingFields) {
    TextConversionContext context =
        new TextConversionContext()
            .setLocale(locale)
            .setNullStrings(nullStrings)
            .setBooleanStrings(booleanStrings)
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
            .setObjectMapper(objectMapper)
            .setAllowExtraFields(allowExtraFields)
            .setAllowMissingFields(allowMissingFields);
    return new ConvertingCodecFactory(context);
  }
}
