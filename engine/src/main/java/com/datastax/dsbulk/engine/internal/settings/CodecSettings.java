/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private final LoaderConfig config;

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
  public CodecSettings(LoaderConfig config) {
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
        throw new BulkConfigurationException(
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
        throw new BulkConfigurationException(
            "Invalid boolean numbers list, expecting two elements, got " + booleanNumbers);
      }
      booleanStrings = config.getStringList(BOOLEAN_STRINGS);

      // UUID
      generator = config.getEnum(TimeUUIDGenerator.class, TIME_UUID_GENERATOR);

      // json
      objectMapper = JsonCodecUtils.getObjectMapper();

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "codec");
    }
  }

  public ExtendedCodecRegistry createCodecRegistry(
      boolean allowExtraFields, boolean allowMissingFields) {
    return new ExtendedCodecRegistryBuilder()
        .withLocale(locale)
        .withNullStrings(nullStrings)
        .withBooleanStrings(booleanStrings)
        .withBooleanNumbers(booleanNumbers.get(0), booleanNumbers.get(1))
        .withNumberFormat(numberFormat)
        .withFormatNumbers(formatNumbers)
        .withOverflowStrategy(overflowStrategy)
        .withRoundingMode(roundingMode)
        .withDateFormat(dateFormat)
        .withTimeFormat(timeFormat)
        .withTimestampFormat(timestampFormat)
        .withTimeZone(timeZone)
        .withTimeUnit(timeUnit)
        .withEpoch(epoch)
        .withUuidGenerator(generator)
        .withObjectMapper(objectMapper)
        .allowExtraFields(allowExtraFields)
        .allowMissingFields(allowMissingFields)
        .build();
  }
}
