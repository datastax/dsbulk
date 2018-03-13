/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToBigDecimalCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToBigIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToBlobCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToByteCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDurationCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToFloatCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToInetAddressCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToLocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToLocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToLongCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToShortCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToTupleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToUDTCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.BooleanToNumberCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToNumberCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBigDecimalCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBigIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBlobCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToByteCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToDurationCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToFloatCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInetAddressCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToShortCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToTupleCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToUDTCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.DateToTemporalCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.DateToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToTemporalCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * If only CodecRegistry were extensible :(
 *
 * <p>This class helps solve the following problem: how to create codecs for combinations of Java
 * types + CQL types that the original CodecRegistry cannot handle?
 */
public class ExtendedCodecRegistry {

  private static final TypeToken<String> STRING_TYPE_TOKEN = TypeToken.of(String.class);
  private static final TypeToken<JsonNode> JSON_NODE_TYPE_TOKEN = TypeToken.of(JsonNode.class);

  private final CodecRegistry codecRegistry;
  private final List<String> nullWords;
  private final Map<String, Boolean> booleanInputWords;
  private final Map<Boolean, String> booleanOutputWords;
  private final List<BigDecimal> booleanNumbers;
  private final FastThreadLocal<NumberFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final DateTimeFormatter localDateFormat;
  private final DateTimeFormatter localTimeFormat;
  private final DateTimeFormatter timestampFormat;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final ObjectMapper objectMapper;
  private final TimeUUIDGenerator generator;

  public ExtendedCodecRegistry(
      CodecRegistry codecRegistry,
      List<String> nullWords,
      Map<String, Boolean> booleanInputWords,
      Map<Boolean, String> booleanOutputWords,
      List<BigDecimal> booleanNumbers,
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter localDateFormat,
      DateTimeFormatter localTimeFormat,
      DateTimeFormatter timestampFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      TimeUUIDGenerator generator,
      ObjectMapper objectMapper) {
    this.codecRegistry = codecRegistry;
    this.nullWords = nullWords;
    this.booleanInputWords = booleanInputWords;
    this.booleanOutputWords = booleanOutputWords;
    this.booleanNumbers = booleanNumbers;
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.localDateFormat = localDateFormat;
    this.localTimeFormat = localTimeFormat;
    this.timestampFormat = timestampFormat;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.generator = generator;
    this.objectMapper = objectMapper;
    // register Java Time API codecs
    codecRegistry.register(LocalDateCodec.instance, LocalTimeCodec.instance, InstantCodec.instance);
  }

  @SuppressWarnings("unchecked")
  public <T> TypeCodec<T> codecFor(
      @NotNull DataType cqlType, @NotNull TypeToken<? extends T> javaType) {
    TypeCodec<T> codec;
    try {
      if (javaType.getRawType().equals(String.class)) {
        // Never return the driver's built-in StringCodec because it does not handle
        // null words. We need StringToStringCodec here.
        codec = (TypeCodec<T>) createStringConvertingCodec(cqlType);
      } else {
        codec = (TypeCodec<T>) codecRegistry.codecFor(cqlType, javaType);
      }
    } catch (CodecNotFoundException e) {
      codec = (TypeCodec<T>) maybeCreateConvertingCodec(cqlType, javaType);
      if (codec == null) {
        throw e;
      }
    }
    return codec;
  }

  @SuppressWarnings("unchecked")
  public <FROM, TO> ConvertingCodec<FROM, TO> convertingCodecFor(
      @NotNull DataType cqlType, @NotNull TypeToken<? extends FROM> javaType) {
    ConvertingCodec<FROM, TO> codec =
        (ConvertingCodec<FROM, TO>) maybeCreateConvertingCodec(cqlType, javaType);
    if (codec != null) {
      return codec;
    }
    throw new CodecNotFoundException(
        String.format(
            "ConvertingCodec not found for requested operation: [%s <-> %s]", cqlType, javaType),
        cqlType,
        javaType);
  }

  @Nullable
  private ConvertingCodec<?, ?> maybeCreateConvertingCodec(
      @NotNull DataType cqlType, @NotNull TypeToken<?> javaType) {
    if (String.class.equals(javaType.getRawType())) {
      return createStringConvertingCodec(cqlType);
    }
    if (JsonNode.class.equals(javaType.getRawType())) {
      return createJsonNodeConvertingCodec(cqlType);
    }
    if (Number.class.isAssignableFrom(javaType.getRawType()) && isNumeric(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) javaType.getRawType();
      return new NumberToNumberCodec<>(numberType, codecRegistry.codecFor(cqlType));
    }
    if (Number.class.isAssignableFrom(javaType.getRawType()) && cqlType == DataType.timestamp()) {
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) javaType.getRawType();
      return new NumberToInstantCodec<>(numberType, timeUnit, epoch);
    }
    if (Number.class.isAssignableFrom(javaType.getRawType()) && isUUID(cqlType)) {
      TypeCodec<UUID> uuidCodec = codecRegistry.codecFor(cqlType);
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) javaType.getRawType();
      NumberToInstantCodec<Number> instantCodec =
          new NumberToInstantCodec<>(numberType, timeUnit, epoch);
      return new NumberToUUIDCodec<>(uuidCodec, instantCodec, generator);
    }
    if (Number.class.isAssignableFrom(javaType.getRawType()) && cqlType == DataType.cboolean()) {
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) javaType.getRawType();
      return new NumberToBooleanCodec<>(numberType, booleanNumbers);
    }
    if (Temporal.class.isAssignableFrom(javaType.getRawType()) && isTemporal(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Temporal> fromTemporalType = (Class<Temporal>) javaType.getRawType();
      if (cqlType == DataType.date()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, LocalDateCodec.instance, timestampFormat.getZone(), epoch);
      }
      if (cqlType == DataType.time()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, LocalTimeCodec.instance, timestampFormat.getZone(), epoch);
      }
      if (cqlType == DataType.timestamp()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, InstantCodec.instance, timestampFormat.getZone(), epoch);
      }
    }
    if (Temporal.class.isAssignableFrom(javaType.getRawType()) && isUUID(cqlType)) {
      TypeCodec<UUID> uuidCodec = codecRegistry.codecFor(cqlType);
      @SuppressWarnings("unchecked")
      TemporalToTemporalCodec<TemporalAccessor, Instant> instantCodec =
          (TemporalToTemporalCodec<TemporalAccessor, Instant>)
              maybeCreateConvertingCodec(DataType.timestamp(), javaType);
      assert instantCodec != null;
      return new TemporalToUUIDCodec<>(uuidCodec, instantCodec, generator);
    }
    if (Date.class.isAssignableFrom(javaType.getRawType()) && isTemporal(cqlType)) {
      if (cqlType == DataType.date()) {
        return new DateToTemporalCodec<>(
            Date.class, LocalDateCodec.instance, timestampFormat.getZone());
      }
      if (cqlType == DataType.time()) {
        return new DateToTemporalCodec<>(
            Date.class, LocalTimeCodec.instance, timestampFormat.getZone());
      }
      if (cqlType == DataType.timestamp()) {
        return new DateToTemporalCodec<>(
            Date.class, InstantCodec.instance, timestampFormat.getZone());
      }
    }
    if (Date.class.isAssignableFrom(javaType.getRawType()) && isUUID(cqlType)) {
      TypeCodec<UUID> uuidCodec = codecRegistry.codecFor(cqlType);
      @SuppressWarnings("unchecked")
      DateToTemporalCodec<Date, Instant> instantCodec =
          (DateToTemporalCodec<Date, Instant>)
              maybeCreateConvertingCodec(DataType.timestamp(), javaType);
      assert instantCodec != null;
      return new DateToUUIDCodec<>(uuidCodec, instantCodec, generator);
    }
    if (Boolean.class.isAssignableFrom(javaType.getRawType()) && isNumeric(cqlType)) {
      return new BooleanToNumberCodec<>(codecRegistry.codecFor(cqlType), booleanNumbers);
    }
    return null;
  }

  private ConvertingCodec<String, ?> createStringConvertingCodec(@NotNull DataType cqlType) {
    DataType.Name name = cqlType.getName();
    switch (name) {
      case ASCII:
      case TEXT:
      case VARCHAR:
        return new StringToStringCodec(codecRegistry.codecFor(cqlType), nullWords);
      case BOOLEAN:
        return new StringToBooleanCodec(booleanInputWords, booleanOutputWords, nullWords);
      case TINYINT:
        return new StringToByteCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case SMALLINT:
        return new StringToShortCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case INT:
        return new StringToIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case BIGINT:
        return new StringToLongCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case FLOAT:
        return new StringToFloatCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DOUBLE:
        return new StringToDoubleCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case VARINT:
        return new StringToBigIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DECIMAL:
        return new StringToBigDecimalCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DATE:
        return new StringToLocalDateCodec(localDateFormat, nullWords);
      case TIME:
        return new StringToLocalTimeCodec(localTimeFormat, nullWords);
      case TIMESTAMP:
        return new StringToInstantCodec(timestampFormat, numberFormat, timeUnit, epoch, nullWords);
      case INET:
        return new StringToInetAddressCodec(nullWords);
      case UUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>) createStringConvertingCodec(DataType.timestamp());
          return new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, generator, nullWords);
        }
      case TIMEUUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>) createStringConvertingCodec(DataType.timestamp());
          return new StringToUUIDCodec(TypeCodec.timeUUID(), instantCodec, generator, nullWords);
        }
      case BLOB:
        return new StringToBlobCodec(nullWords);
      case DURATION:
        return new StringToDurationCodec(nullWords);
      case LIST:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToListCodec<Object> jsonCodec =
              (JsonNodeToListCodec<Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToListCodec<>(jsonCodec, objectMapper, nullWords);
        }
      case SET:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToSetCodec<Object> jsonCodec =
              (JsonNodeToSetCodec<Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToSetCodec<>(jsonCodec, objectMapper, nullWords);
        }
      case MAP:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToMapCodec<Object, Object> jsonCodec =
              (JsonNodeToMapCodec<Object, Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToMapCodec<>(jsonCodec, objectMapper, nullWords);
        }
      case TUPLE:
        {
          JsonNodeToTupleCodec jsonCodec =
              (JsonNodeToTupleCodec) createJsonNodeConvertingCodec(cqlType);
          return new StringToTupleCodec(jsonCodec, objectMapper, nullWords);
        }
      case UDT:
        {
          JsonNodeToUDTCodec jsonCodec =
              (JsonNodeToUDTCodec) createJsonNodeConvertingCodec(cqlType);
          return new StringToUDTCodec(jsonCodec, objectMapper, nullWords);
        }
      case COUNTER:
      case CUSTOM:
      default:
        String msg =
            String.format(
                "Codec not found for requested operation: [%s <-> %s]", cqlType, String.class);
        throw new CodecNotFoundException(msg, cqlType, STRING_TYPE_TOKEN);
    }
  }

  private ConvertingCodec<JsonNode, ?> createJsonNodeConvertingCodec(@NotNull DataType cqlType) {
    DataType.Name name = cqlType.getName();
    switch (name) {
      case ASCII:
      case TEXT:
      case VARCHAR:
        return new JsonNodeToStringCodec(codecRegistry.codecFor(cqlType), nullWords);
      case BOOLEAN:
        return new JsonNodeToBooleanCodec(booleanInputWords, nullWords);
      case TINYINT:
        return new JsonNodeToByteCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case SMALLINT:
        return new JsonNodeToShortCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case INT:
        return new JsonNodeToIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case BIGINT:
        return new JsonNodeToLongCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case FLOAT:
        return new JsonNodeToFloatCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DOUBLE:
        return new JsonNodeToDoubleCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case VARINT:
        return new JsonNodeToBigIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DECIMAL:
        return new JsonNodeToBigDecimalCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullWords);
      case DATE:
        return new JsonNodeToLocalDateCodec(localDateFormat, nullWords);
      case TIME:
        return new JsonNodeToLocalTimeCodec(localTimeFormat, nullWords);
      case TIMESTAMP:
        return new JsonNodeToInstantCodec(
            timestampFormat, numberFormat, timeUnit, epoch, nullWords);
      case INET:
        return new JsonNodeToInetAddressCodec(nullWords);
      case UUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>) createStringConvertingCodec(DataType.timestamp());
          return new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, generator, nullWords);
        }
      case TIMEUUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>) createStringConvertingCodec(DataType.timestamp());
          return new JsonNodeToUUIDCodec(TypeCodec.timeUUID(), instantCodec, generator, nullWords);
        }
      case BLOB:
        return new JsonNodeToBlobCodec(nullWords);
      case DURATION:
        return new JsonNodeToDurationCodec(nullWords);
      case LIST:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<List<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType);
          return new JsonNodeToListCodec<>(collectionCodec, eltCodec, objectMapper, nullWords);
        }
      case SET:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<Set<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType);
          return new JsonNodeToSetCodec<>(collectionCodec, eltCodec, objectMapper, nullWords);
        }
      case MAP:
        {
          DataType keyType = cqlType.getTypeArguments().get(0);
          DataType valueType = cqlType.getTypeArguments().get(1);
          TypeCodec<Map<Object, Object>> mapCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Object> keyCodec =
              (ConvertingCodec<String, Object>) createStringConvertingCodec(keyType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> valueCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(valueType);
          return new JsonNodeToMapCodec<>(mapCodec, keyCodec, valueCodec, objectMapper, nullWords);
        }
      case TUPLE:
        {
          TypeCodec<TupleValue> tupleCodec = codecRegistry.codecFor(cqlType);
          ImmutableList.Builder<ConvertingCodec<JsonNode, Object>> eltCodecs =
              new ImmutableList.Builder<>();
          for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
            @SuppressWarnings("unchecked")
            ConvertingCodec<JsonNode, Object> eltCodec =
                (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(eltType);
            eltCodecs.add(eltCodec);
          }
          return new JsonNodeToTupleCodec(tupleCodec, eltCodecs.build(), objectMapper, nullWords);
        }
      case UDT:
        {
          TypeCodec<UDTValue> udtCodec = codecRegistry.codecFor(cqlType);
          ImmutableMap.Builder<String, ConvertingCodec<JsonNode, Object>> fieldCodecs =
              new ImmutableMap.Builder<>();
          for (UserType.Field field : ((UserType) cqlType)) {
            @SuppressWarnings("unchecked")
            ConvertingCodec<JsonNode, Object> fieldCodec =
                (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(field.getType());
            fieldCodecs.put(field.getName(), fieldCodec);
          }
          return new JsonNodeToUDTCodec(udtCodec, fieldCodecs.build(), objectMapper, nullWords);
        }
      case COUNTER:
      case CUSTOM:
      default:
        String msg =
            String.format(
                "Codec not found for requested operation: [%s <-> %s]", cqlType, JsonNode.class);
        throw new CodecNotFoundException(msg, cqlType, JSON_NODE_TYPE_TOKEN);
    }
  }

  private static boolean isNumeric(@NotNull DataType cqlType) {
    return cqlType == DataType.tinyint()
        || cqlType == DataType.smallint()
        || cqlType == DataType.cint()
        || cqlType == DataType.bigint()
        || cqlType == DataType.cfloat()
        || cqlType == DataType.cdouble()
        || cqlType == DataType.varint()
        || cqlType == DataType.decimal();
  }

  private static boolean isTemporal(@NotNull DataType cqlType) {
    return cqlType == DataType.date()
        || cqlType == DataType.time()
        || cqlType == DataType.timestamp();
  }

  private static boolean isUUID(@NotNull DataType cqlType) {
    return cqlType == DataType.uuid() || cqlType == DataType.timeuuid();
  }
}
