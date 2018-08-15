/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs;

import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class CodecTestUtils {
  private static final CodecRegistry CODEC_REGISTRY = new DefaultCodecRegistry("test");

  public static ExtendedCodecRegistry newCodecRegistry(String conf) {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(conf)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    return settings.createCodecRegistry(new DefaultCodecRegistry("test"));
  }

  public static TupleType newTupleType(DataType... types) {
    return newTupleType(DseProtocolVersion.DSE_V2, CODEC_REGISTRY, types);
  }

  public static TupleType newTupleType(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types) {
    return new DefaultTupleType(
        Arrays.asList(types),
        new AttachmentPoint() {
          @Override
          @NotNull
          public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
          }

          @Override
          @NotNull
          public CodecRegistry getCodecRegistry() {
            return codecRegistry;
          }
        });
  }

  /**
   * Static copy of the actual CodecSettings class in engine. Used for creating a codec registry
   * with certain settings.
   */
  private static class CodecSettings {

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
            CodecUtils.getNumberFormatThreadLocal(
                config.getString(NUMBER), locale, roundingMode, formatNumbers);

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
        localDateFormat = CodecUtils.getTemporalFormat(config.getString(DATE), null, locale);
        localTimeFormat = CodecUtils.getTemporalFormat(config.getString(TIME), null, locale);
        timestampFormat =
            CodecUtils.getTemporalFormat(config.getString(TIMESTAMP), timeZone, locale);

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
        objectMapper = JsonCodecUtils.getObjectMapper();

      } catch (ConfigException e) {
        throw ConfigUtils.configExceptionToBulkConfigurationException(e, "codec");
      }
    }

    ExtendedCodecRegistry createCodecRegistry(CodecRegistry codecRegistry) {
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
}
