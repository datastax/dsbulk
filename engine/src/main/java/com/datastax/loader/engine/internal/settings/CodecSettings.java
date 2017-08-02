/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.loader.engine.internal.codecs.NumberToNumberCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigDecimalCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToBooleanCodec;
import com.datastax.loader.engine.internal.codecs.StringToByteCodec;
import com.datastax.loader.engine.internal.codecs.StringToDoubleCodec;
import com.datastax.loader.engine.internal.codecs.StringToFloatCodec;
import com.datastax.loader.engine.internal.codecs.StringToInetAddressCodec;
import com.datastax.loader.engine.internal.codecs.StringToInstantCodec;
import com.datastax.loader.engine.internal.codecs.StringToIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalDateCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalTimeCodec;
import com.datastax.loader.engine.internal.codecs.StringToLongCodec;
import com.datastax.loader.engine.internal.codecs.StringToShortCodec;
import com.datastax.loader.engine.internal.codecs.StringToUUIDCodec;
import com.datastax.loader.engine.internal.codecs.TemporalToTemporalCodec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

  private static final Set<TypeCodec<?>> NUMERIC_CODECS =
      Sets.newHashSet(
          TypeCodec.tinyInt(),
          TypeCodec.smallInt(),
          TypeCodec.cint(),
          TypeCodec.bigint(),
          TypeCodec.cfloat(),
          TypeCodec.cdouble(),
          TypeCodec.varint(),
          TypeCodec.decimal());

  private final Config config;

  public CodecSettings(Config config) {
    this.config = config;
  }

  public void registerCodecs(Cluster cluster) {
    Locale locale = parseLocale(config.getString("locale"));
    Map<String, Boolean> booleanInputs = getBooleanInputs(config.getStringList("boolean"));
    Map<Boolean, String> booleanOutputs = getBooleanOutputs(config.getStringList("boolean"));
    ThreadLocal<DecimalFormat> numberFormat = getNumberFormat(locale, config.getString("number"));
    String timeZone = config.getString("time-zone");
    DateTimeFormatter localDateFormat = getDateFormat(config.getString("date"), timeZone, locale);
    DateTimeFormatter localTimeFormat = getDateFormat(config.getString("time"), timeZone, locale);
    DateTimeFormatter timestampFormat =
        getDateFormat(config.getString("timestamp"), timeZone, locale);
    CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
    // JDK8 codecs
    codecRegistry.register(LocalDateCodec.instance, LocalTimeCodec.instance, InstantCodec.instance);
    // String to X codecs
    codecRegistry
        .register(new StringToBooleanCodec(booleanInputs, booleanOutputs))
        .register(new StringToByteCodec(numberFormat))
        .register(new StringToShortCodec(numberFormat))
        .register(new StringToIntegerCodec(numberFormat))
        .register(new StringToLongCodec(numberFormat))
        .register(new StringToFloatCodec(numberFormat))
        .register(new StringToDoubleCodec(numberFormat))
        .register(new StringToBigIntegerCodec(numberFormat))
        .register(new StringToBigDecimalCodec(numberFormat))
        .register(new StringToLocalDateCodec(localDateFormat))
        .register(new StringToLocalTimeCodec(localTimeFormat))
        .register(new StringToInstantCodec(timestampFormat))
        .register(StringToInetAddressCodec.INSTANCE)
        .register(new StringToUUIDCodec(TypeCodec.uuid()))
        .register(new StringToUUIDCodec(TypeCodec.timeUUID()));
    // Number to Number codecs
    for (TypeCodec<?> codec1 : NUMERIC_CODECS) {
      for (TypeCodec<?> codec2 : NUMERIC_CODECS) {
        if (codec1 == codec2) continue;
        @SuppressWarnings("unchecked")
        NumberToNumberCodec<Number, Number> codec =
            new NumberToNumberCodec<>(
                (Class<Number>) codec1.getJavaType().getRawType(), (TypeCodec<Number>) codec2);
        codecRegistry.register(codec);
      }
    }
    // Temporal to Temporal codecs
    codecRegistry.register(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalDateCodec.instance));
    codecRegistry.register(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalTimeCodec.instance));
    codecRegistry.register(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, InstantCodec.instance));
    codecRegistry.register(new TemporalToTemporalCodec<>(Instant.class, LocalDateCodec.instance));
    codecRegistry.register(new TemporalToTemporalCodec<>(Instant.class, LocalTimeCodec.instance));
    codecRegistry.register(
        new TemporalToTemporalCodec<>(LocalDateTime.class, LocalDateCodec.instance));
    codecRegistry.register(
        new TemporalToTemporalCodec<>(LocalDateTime.class, LocalTimeCodec.instance));
    codecRegistry.register(
        new TemporalToTemporalCodec<>(LocalDateTime.class, InstantCodec.instance));
    codecRegistry.register(new TemporalToTemporalCodec<>(LocalDate.class, InstantCodec.instance));
    codecRegistry.register(new TemporalToTemporalCodec<>(LocalTime.class, InstantCodec.instance));
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
