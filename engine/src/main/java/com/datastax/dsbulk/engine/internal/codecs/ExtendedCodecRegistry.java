/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToNumberCodec;
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
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToTemporalCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Map<String, Boolean> booleanInputs;
  private final Map<Boolean, String> booleanOutputs;
  private final ThreadLocal<DecimalFormat> numberFormat;
  private final DateTimeFormatter localDateFormat;
  private final DateTimeFormatter localTimeFormat;
  private final DateTimeFormatter timestampFormat;
  private final TimeUnit numericTimestampUnit;
  private final Instant numericTimestampEpoch;
  private final ObjectMapper objectMapper;

  public ExtendedCodecRegistry(
      CodecRegistry codecRegistry,
      Map<String, Boolean> booleanInputs,
      Map<Boolean, String> booleanOutputs,
      ThreadLocal<DecimalFormat> numberFormat,
      DateTimeFormatter localDateFormat,
      DateTimeFormatter localTimeFormat,
      DateTimeFormatter timestampFormat,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      ObjectMapper objectMapper) {
    this.codecRegistry = codecRegistry;
    this.booleanInputs = booleanInputs;
    this.booleanOutputs = booleanOutputs;
    this.numberFormat = numberFormat;
    this.localDateFormat = localDateFormat;
    this.localTimeFormat = localTimeFormat;
    this.timestampFormat = timestampFormat;
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
    this.objectMapper = objectMapper;
    // register Java Time API codecs
    codecRegistry.register(LocalDateCodec.instance, LocalTimeCodec.instance, InstantCodec.instance);
  }

  @SuppressWarnings("unchecked")
  public <T> TypeCodec<T> codecFor(
      @NotNull DataType cqlType, @NotNull TypeToken<? extends T> javaType) {
    try {
      return (TypeCodec<T>) codecRegistry.codecFor(cqlType, javaType);
    } catch (CodecNotFoundException e) {
      TypeCodec<T> codec = (TypeCodec<T>) maybeCreateConvertingCodec(cqlType, javaType);
      if (codec != null) {
        return codec;
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  public <FROM, TO> ConvertingCodec<FROM, TO> convertingCodecFor(
      @NotNull DataType cqlType, @NotNull TypeToken<FROM> javaType) {
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
    if (Temporal.class.isAssignableFrom(javaType.getRawType()) && isTemporal(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Temporal> fromTemporalType = (Class<Temporal>) javaType.getRawType();
      if (cqlType == DataType.date()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, LocalDateCodec.instance, timestampFormat.getZone());
      }
      if (cqlType == DataType.time()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, LocalTimeCodec.instance, timestampFormat.getZone());
      }
      if (cqlType == DataType.timestamp()) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, InstantCodec.instance, timestampFormat.getZone());
      }
    }
    if (Date.class.isAssignableFrom(javaType.getRawType())) {
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
    return null;
  }

  private ConvertingCodec<String, ?> createStringConvertingCodec(@NotNull DataType cqlType) {
    DataType.Name name = cqlType.getName();
    switch (name) {
      case ASCII:
      case TEXT:
      case VARCHAR:
        return new StringToStringCodec(codecRegistry.codecFor(cqlType));
      case BOOLEAN:
        return new StringToBooleanCodec(booleanInputs, booleanOutputs);
      case TINYINT:
        return new StringToByteCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case SMALLINT:
        return new StringToShortCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case INT:
        return new StringToIntegerCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case BIGINT:
        return new StringToLongCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case FLOAT:
        return new StringToFloatCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DOUBLE:
        return new StringToDoubleCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case VARINT:
        return new StringToBigIntegerCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DECIMAL:
        return new StringToBigDecimalCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DATE:
        return new StringToLocalDateCodec(localDateFormat);
      case TIME:
        return new StringToLocalTimeCodec(localTimeFormat);
      case TIMESTAMP:
        return new StringToInstantCodec(
            timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case INET:
        return StringToInetAddressCodec.INSTANCE;
      case UUID:
        return new StringToUUIDCodec(TypeCodec.uuid());
      case TIMEUUID:
        return new StringToUUIDCodec(TypeCodec.timeUUID());
      case BLOB:
        return StringToBlobCodec.INSTANCE;
      case DURATION:
        return StringToDurationCodec.INSTANCE;
      case LIST:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToListCodec<Object> jsonCodec =
              (JsonNodeToListCodec<Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToListCodec<>(jsonCodec, objectMapper);
        }
      case SET:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToSetCodec<Object> jsonCodec =
              (JsonNodeToSetCodec<Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToSetCodec<>(jsonCodec, objectMapper);
        }
      case MAP:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToMapCodec<Object, Object> jsonCodec =
              (JsonNodeToMapCodec<Object, Object>) createJsonNodeConvertingCodec(cqlType);
          return new StringToMapCodec<>(jsonCodec, objectMapper);
        }
      case TUPLE:
        {
          JsonNodeToTupleCodec jsonCodec =
              (JsonNodeToTupleCodec) createJsonNodeConvertingCodec(cqlType);
          return new StringToTupleCodec(jsonCodec, objectMapper);
        }
      case UDT:
        {
          JsonNodeToUDTCodec jsonCodec =
              (JsonNodeToUDTCodec) createJsonNodeConvertingCodec(cqlType);
          return new StringToUDTCodec(jsonCodec, objectMapper);
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
        return new JsonNodeToStringCodec(codecRegistry.codecFor(cqlType));
      case BOOLEAN:
        return new JsonNodeToBooleanCodec(booleanInputs);
      case TINYINT:
        return new JsonNodeToByteCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case SMALLINT:
        return new JsonNodeToShortCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case INT:
        return new JsonNodeToIntegerCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case BIGINT:
        return new JsonNodeToLongCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case FLOAT:
        return new JsonNodeToFloatCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DOUBLE:
        return new JsonNodeToDoubleCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case VARINT:
        return new JsonNodeToBigIntegerCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DECIMAL:
        return new JsonNodeToBigDecimalCodec(
            numberFormat, timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case DATE:
        return new JsonNodeToLocalDateCodec(localDateFormat);
      case TIME:
        return new JsonNodeToLocalTimeCodec(localTimeFormat);
      case TIMESTAMP:
        return new JsonNodeToInstantCodec(
            timestampFormat, numericTimestampUnit, numericTimestampEpoch);
      case INET:
        return JsonNodeToInetAddressCodec.INSTANCE;
      case UUID:
        return new JsonNodeToUUIDCodec(TypeCodec.uuid());
      case TIMEUUID:
        return new JsonNodeToUUIDCodec(TypeCodec.timeUUID());
      case BLOB:
        return JsonNodeToBlobCodec.INSTANCE;
      case DURATION:
        return JsonNodeToDurationCodec.INSTANCE;
      case LIST:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<List<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType);
          return new JsonNodeToListCodec<>(collectionCodec, eltCodec, objectMapper);
        }
      case SET:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<Set<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType);
          return new JsonNodeToSetCodec<>(collectionCodec, eltCodec, objectMapper);
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
          return new JsonNodeToMapCodec<>(mapCodec, keyCodec, valueCodec, objectMapper);
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
          return new JsonNodeToTupleCodec(tupleCodec, eltCodecs.build(), objectMapper);
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
          return new JsonNodeToUDTCodec(udtCodec, fieldCodecs.build(), objectMapper);
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
}
