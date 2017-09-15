/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.driver.core.DataType.Name.ASCII;
import static com.datastax.driver.core.DataType.Name.TEXT;
import static com.datastax.driver.core.DataType.Name.VARCHAR;

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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final CodecRegistry codecRegistry;
  private final Map<String, Boolean> booleanInputs;
  private final Map<Boolean, String> booleanOutputs;
  private final ThreadLocal<DecimalFormat> numberFormat;
  private final DateTimeFormatter localDateFormat;
  private final DateTimeFormatter localTimeFormat;
  private final DateTimeFormatter timestampFormat;
  private final String delimiter;
  private final String keyValueSeparator;

  public ExtendedCodecRegistry(
      CodecRegistry codecRegistry,
      Map<String, Boolean> booleanInputs,
      Map<Boolean, String> booleanOutputs,
      ThreadLocal<DecimalFormat> numberFormat,
      DateTimeFormatter localDateFormat,
      DateTimeFormatter localTimeFormat,
      DateTimeFormatter timestampFormat,
      String delimiter,
      String keyValueSeparator) {
    this.codecRegistry = codecRegistry;
    this.booleanInputs = booleanInputs;
    this.booleanOutputs = booleanOutputs;
    this.numberFormat = numberFormat;
    this.localDateFormat = localDateFormat;
    this.localTimeFormat = localTimeFormat;
    this.timestampFormat = timestampFormat;
    this.delimiter = delimiter;
    this.keyValueSeparator = keyValueSeparator;
  }

  @SuppressWarnings("unchecked")
  public <T> TypeCodec<T> codecFor(
      @NotNull DataType cqlType, @NotNull TypeToken<? extends T> javaType) {
    try {
      return (TypeCodec<T>) codecRegistry.codecFor(cqlType, javaType);
    } catch (CodecNotFoundException e) {
      TypeCodec<T> codec = (TypeCodec<T>) maybeCreateCodec(cqlType, javaType);
      if (codec != null) {
        codecRegistry.register(codec);
        return codec;
      }
      throw e;
    }
  }

  @Nullable
  private TypeCodec<?> maybeCreateCodec(@NotNull DataType cqlType, @NotNull TypeToken<?> javaType) {
    if (cqlType == DataType.date() && javaType.getRawType().equals(LocalDate.class)) {
      return LocalDateCodec.instance;
    }
    if (cqlType == DataType.time() && javaType.getRawType().equals(LocalTimeCodec.class)) {
      return LocalTimeCodec.instance;
    }
    if (cqlType == DataType.timestamp() && javaType.getRawType().equals(Instant.class)) {
      return InstantCodec.instance;
    }
    if (String.class.equals(javaType.getRawType())) {
      return createStringConvertingCodec(cqlType);
    }
    if (Number.class.isAssignableFrom(javaType.getRawType()) && isNumeric(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) javaType.getRawType();
      return new NumberToNumberCodec<>(numberType, codecRegistry.codecFor(cqlType));
    }
    if (Temporal.class.isAssignableFrom(javaType.getRawType()) && isTemporal(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Temporal> temporalType = (Class<Temporal>) javaType.getRawType();
      return new TemporalToTemporalCodec<>(temporalType, codecRegistry.codecFor(cqlType));
    }
    return null;
  }

  private TypeCodec<?> createStringConvertingCodec(@NotNull DataType cqlType) {
    DataType.Name name = cqlType.getName();
    switch (name) {
      case BOOLEAN:
        return new StringToBooleanCodec(booleanInputs, booleanOutputs);
      case TINYINT:
        return new StringToByteCodec(numberFormat);
      case SMALLINT:
        return new StringToShortCodec(numberFormat);
      case INT:
        return new StringToIntegerCodec(numberFormat);
      case BIGINT:
        return new StringToLongCodec(numberFormat);
      case FLOAT:
        return new StringToFloatCodec(numberFormat);
      case DOUBLE:
        return new StringToDoubleCodec(numberFormat);
      case VARINT:
        return new StringToBigIntegerCodec(numberFormat);
      case DECIMAL:
        return new StringToBigDecimalCodec(numberFormat);
      case DATE:
        return new StringToLocalDateCodec(localDateFormat);
      case TIME:
        return new StringToLocalTimeCodec(localTimeFormat);
      case TIMESTAMP:
        return new StringToInstantCodec(timestampFormat);
      case INET:
        return StringToInetAddressCodec.INSTANCE;
      case UUID:
        return new StringToUUIDCodec(TypeCodec.uuid());
      case TIMEUUID:
        return new StringToUUIDCodec(TypeCodec.timeUUID());
      case LIST:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<List<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          ConvertingCodec<String, Object> eltCodec = stringConvertingCodecFor(elementType);
          return new StringToListCodec<>(collectionCodec, eltCodec, delimiter);
        }
      case SET:
        {
          DataType elementType = cqlType.getTypeArguments().get(0);
          TypeCodec<Set<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          ConvertingCodec<String, Object> eltCodec = stringConvertingCodecFor(elementType);
          return new StringToSetCodec<>(collectionCodec, eltCodec, delimiter);
        }
      case MAP:
        {
          DataType keyType = cqlType.getTypeArguments().get(0);
          DataType valueType = cqlType.getTypeArguments().get(1);
          TypeCodec<Map<Object, Object>> mapCodec = codecRegistry.codecFor(cqlType);
          ConvertingCodec<String, Object> keyCodec = stringConvertingCodecFor(keyType);
          ConvertingCodec<String, Object> valueCodec = stringConvertingCodecFor(valueType);
          return new StringToMapCodec<>(
              mapCodec, keyCodec, valueCodec, delimiter, keyValueSeparator);
        }
      case TUPLE:
        {
          TypeCodec<TupleValue> tupleCodec = codecRegistry.codecFor(cqlType);
          ImmutableList.Builder<ConvertingCodec<String, Object>> eltCodecs =
              new ImmutableList.Builder<>();
          for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
            eltCodecs.add(stringConvertingCodecFor(eltType));
          }
          return new StringToTupleCodec(tupleCodec, eltCodecs.build(), delimiter);
        }
      case UDT:
        {
          TypeCodec<UDTValue> udtCodec = codecRegistry.codecFor(cqlType);
          ImmutableMap.Builder<String, ConvertingCodec<String, Object>> fieldCodecs =
              new ImmutableMap.Builder<>();
          for (UserType.Field field : ((UserType) cqlType)) {
            fieldCodecs.put(field.getName(), stringConvertingCodecFor(field.getType()));
          }
          return new StringToUDTCodec(udtCodec, fieldCodecs.build(), delimiter, keyValueSeparator);
        }
      default:
        throw new AssertionError();
    }
  }

  private <T> ConvertingCodec<String, T> stringConvertingCodecFor(DataType cqlType) {
    // for text CQL types that map naturally to String, we need to wrap the returned codec in a dummy converting codec
    if (cqlType.getName() == VARCHAR || cqlType.getName() == TEXT || cqlType.getName() == ASCII) {
      //noinspection unchecked
      return (ConvertingCodec<String, T>) new StringToStringCodec(codecRegistry.codecFor(cqlType));
    }
    // for all other CQL types it is guaranteed to be a converting codec
    return (ConvertingCodec<String, T>) codecFor(cqlType, STRING_TYPE_TOKEN);
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

  @VisibleForTesting
  static class StringToStringCodec extends ConvertingCodec<String, String> {

    @VisibleForTesting
    StringToStringCodec(TypeCodec<String> innerCodec) {
      super(innerCodec, String.class);
    }

    @Override
    protected String convertFrom(String s) {
      return s;
    }

    @Override
    protected String convertTo(String value) {
      return value;
    }
  }
}
