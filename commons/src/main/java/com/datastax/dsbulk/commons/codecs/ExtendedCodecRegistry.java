/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.COUNTER;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.CUSTOM;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DURATION;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.LIST;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.MAP;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMEUUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TINYINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TUPLE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UDT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARINT;

import com.datastax.dsbulk.commons.codecs.collection.CollectionToCollectionCodec;
import com.datastax.dsbulk.commons.codecs.collection.CollectionToTupleCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToBigDecimalCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToBigIntegerCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToBlobCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToBooleanCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToByteCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToDurationCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToFloatCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToInetAddressCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToInstantCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToIntegerCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToLocalDateCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToLocalTimeCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToLongCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToMapCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToSetCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToShortCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToTupleCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToUDTCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToUnknownTypeCodec;
import com.datastax.dsbulk.commons.codecs.json.dse.JsonNodeToDateRangeCodec;
import com.datastax.dsbulk.commons.codecs.json.dse.JsonNodeToLineStringCodec;
import com.datastax.dsbulk.commons.codecs.json.dse.JsonNodeToPointCodec;
import com.datastax.dsbulk.commons.codecs.json.dse.JsonNodeToPolygonCodec;
import com.datastax.dsbulk.commons.codecs.number.BooleanToNumberCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToBooleanCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToInstantCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToNumberCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBigDecimalCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBigIntegerCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBlobCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBooleanCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToByteCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToDoubleCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToDurationCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToFloatCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToInetAddressCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToListCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLocalDateCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLocalTimeCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToMapCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToSetCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToShortCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToStringCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToTupleCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUDTCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUnknownTypeCodec;
import com.datastax.dsbulk.commons.codecs.string.dse.StringToDateRangeCodec;
import com.datastax.dsbulk.commons.codecs.string.dse.StringToLineStringCodec;
import com.datastax.dsbulk.commons.codecs.string.dse.StringToPointCodec;
import com.datastax.dsbulk.commons.codecs.string.dse.StringToPolygonCodec;
import com.datastax.dsbulk.commons.codecs.temporal.DateToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.temporal.DateToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.temporal.TemporalToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.temporal.TemporalToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.dse.driver.api.core.codec.geometry.LineStringCodec;
import com.datastax.dse.driver.api.core.codec.geometry.PointCodec;
import com.datastax.dse.driver.api.core.codec.geometry.PolygonCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If only CodecRegistry were extensible :(
 *
 * <p>This class helps solve the following problem: how to create codecs for combinations of Java
 * types + CQL types that the original CodecRegistry cannot handle?
 */
public class ExtendedCodecRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExtendedCodecRegistry.class);
  private static final String DATE_RANGE_CLASS_NAME =
      "org.apache.cassandra.db.marshal.DateRangeType";

  private final CodecRegistry codecRegistry;
  private final List<String> nullStrings;
  private final Map<String, Boolean> booleanInputWords;
  private final Map<Boolean, String> booleanOutputWords;
  private final List<BigDecimal> booleanNumbers;
  private final FastThreadLocal<NumberFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final TemporalFormat localDateFormat;
  private final TemporalFormat localTimeFormat;
  private final TemporalFormat timestampFormat;
  private final ZoneId timeZone;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final ObjectMapper objectMapper;
  private final TimeUUIDGenerator generator;

  public ExtendedCodecRegistry(
      CodecRegistry codecRegistry,
      List<String> nullStrings,
      Map<String, Boolean> booleanInputWords,
      Map<Boolean, String> booleanOutputWords,
      List<BigDecimal> booleanNumbers,
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat localDateFormat,
      TemporalFormat localTimeFormat,
      TemporalFormat timestampFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      TimeUUIDGenerator generator,
      ObjectMapper objectMapper) {
    this.codecRegistry = codecRegistry;
    this.nullStrings = nullStrings;
    this.booleanInputWords = booleanInputWords;
    this.booleanOutputWords = booleanOutputWords;
    this.booleanNumbers = booleanNumbers;
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.localDateFormat = localDateFormat;
    this.localTimeFormat = localTimeFormat;
    this.timestampFormat = timestampFormat;
    this.timeZone = timeZone;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.generator = generator;
    this.objectMapper = objectMapper;
    // register Java Time API codecs
    //    codecRegistry.register(LocalDateCodec.instance, LocalTimeCodec.instance,
    // InstantCodec.instance);
  }

  @SuppressWarnings("unchecked")
  public <T> TypeCodec<T> codecFor(
      @NotNull DataType cqlType, @NotNull GenericType<? extends T> javaType) {
    // Implementation note: it's not required to cache codecs created on-the-fly by this method
    // as caching is meant to be handled by the caller, see
    // com.datastax.dsbulk.engine.internal.schema.DefaultMapping.codec()
    TypeCodec<T> codec;
    try {
      if (GenericType.STRING.equals(javaType)) {
        // Never return the driver's built-in StringCodec because it does not handle
        // null words. We need StringToStringCodec here.
        codec = (TypeCodec<T>) createStringConvertingCodec(cqlType, true);
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
  public <EXTERNAL, INTERNAL> ConvertingCodec<EXTERNAL, INTERNAL> convertingCodecFor(
      @NotNull DataType cqlType, @NotNull GenericType<? extends EXTERNAL> javaType) {
    ConvertingCodec<EXTERNAL, INTERNAL> codec =
        (ConvertingCodec<EXTERNAL, INTERNAL>) maybeCreateConvertingCodec(cqlType, javaType);
    if (codec != null) {
      return codec;
    }
    throw new CodecNotFoundException(
        new RuntimeException(
            String.format(
                "ConvertingCodec not found for requested operation: [%s <-> %s]",
                cqlType, javaType)),
        cqlType,
        javaType);
  }

  @Nullable
  private ConvertingCodec<?, ?> maybeCreateConvertingCodec(
      @NotNull DataType cqlType, @NotNull GenericType<?> javaType) {
    if (GenericType.STRING.equals(javaType)) {
      return createStringConvertingCodec(cqlType, true);
    }
    if (GenericType.of(JsonNode.class).equals(javaType)) {
      return createJsonNodeConvertingCodec(cqlType, true);
    }
    if (javaType.isSubtypeOf(GenericType.of(Collection.class))) {
      GenericType<?> componentType =
          GenericType.of(((ParameterizedType) javaType.getType()).getActualTypeArguments()[0]);
      @SuppressWarnings("unchecked")
      Class<Collection> collType = (Class<Collection>) javaType.getRawType();

      if (isCollection(cqlType)) {
        TypeCodec<?> typeCodec = codecFor(cqlType);

        TypeCodec<?> elementCodec = null;
        Supplier<? extends Collection> collectionCreator;
        if (cqlType instanceof SetType) {
          elementCodec = codecFor(((SetType) cqlType).getElementType(), componentType);
        } else if (cqlType instanceof ListType) {
          elementCodec = codecFor(((ListType) cqlType).getElementType(), componentType);
        }
        if (cqlType instanceof SetType) {
          collectionCreator = HashSet::new;
        } else {
          collectionCreator = ArrayList::new;
        }
        //noinspection unchecked
        return new CollectionToCollectionCodec(
            collType, typeCodec, elementCodec, collectionCreator);
      } else if (cqlType instanceof TupleType) {
        @SuppressWarnings("unchecked")
        TypeCodec<TupleValue> tupleCodec = (TypeCodec<TupleValue>) codecFor(cqlType);
        ImmutableList.Builder<TypeCodec<?>> eltCodecs = new ImmutableList.Builder<>();
        for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
          @SuppressWarnings("unchecked")
          TypeCodec<?> eltCodec = codecFor(eltType, componentType);
          eltCodecs.add(eltCodec);
        }
        //noinspection unchecked
        return new CollectionToTupleCodec(collType, tupleCodec, eltCodecs.build());
      }
    }

    if (javaType.isSubtypeOf(GenericType.of(Number.class))) {
      if (isNumeric(cqlType)) {
        @SuppressWarnings("unchecked")
        Class<Number> numberType = (Class<Number>) javaType.getRawType();
        @SuppressWarnings("unchecked")
        TypeCodec<Number> typeCodec = (TypeCodec<Number>) codecFor(cqlType);
        return new NumberToNumberCodec<>(numberType, typeCodec);
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        @SuppressWarnings("unchecked")
        Class<Number> numberType = (Class<Number>) javaType.getRawType();
        return new NumberToInstantCodec<>(numberType, timeUnit, epoch);
      }
      if (isUUID(cqlType)) {
        @SuppressWarnings("unchecked")
        TypeCodec<UUID> uuidCodec = (TypeCodec<UUID>) codecFor(cqlType);
        @SuppressWarnings("unchecked")
        Class<Number> numberType = (Class<Number>) javaType.getRawType();
        NumberToInstantCodec<Number> instantCodec =
            new NumberToInstantCodec<>(numberType, timeUnit, epoch);
        return new NumberToUUIDCodec<>(uuidCodec, instantCodec, generator);
      }
      if (cqlType == DataTypes.BOOLEAN) {
        @SuppressWarnings("unchecked")
        Class<Number> numberType = (Class<Number>) javaType.getRawType();
        return new NumberToBooleanCodec<>(numberType, booleanNumbers);
      }
    }

    GenericType<?> temporalGenericType = GenericType.of(Temporal.class);
    if (javaType.isSubtypeOf(temporalGenericType) && isTemporal(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Temporal> fromTemporalType = (Class<Temporal>) javaType.getRawType();
      if (cqlType == DataTypes.DATE) {
        return new TemporalToTemporalCodec<>(fromTemporalType, TypeCodecs.DATE, timeZone, epoch);
      }
      if (cqlType == DataTypes.TIME) {
        return new TemporalToTemporalCodec<>(fromTemporalType, TypeCodecs.TIME, timeZone, epoch);
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        return new TemporalToTemporalCodec<>(
            fromTemporalType, TypeCodecs.TIMESTAMP, timeZone, epoch);
      }
    }
    if (javaType.isSubtypeOf(temporalGenericType) && isUUID(cqlType)) {
      @SuppressWarnings("unchecked")
      TypeCodec<UUID> uuidCodec = (TypeCodec<UUID>) codecFor(cqlType);
      @SuppressWarnings("unchecked")
      TemporalToTemporalCodec<TemporalAccessor, Instant> instantCodec =
          (TemporalToTemporalCodec<TemporalAccessor, Instant>)
              maybeCreateConvertingCodec(DataTypes.TIMESTAMP, javaType);
      assert instantCodec != null;
      return new TemporalToUUIDCodec<>(uuidCodec, instantCodec, generator);
    }

    GenericType<Date> dateGenericType = GenericType.of(Date.class);
    if (javaType.isSubtypeOf(dateGenericType) && isTemporal(cqlType)) {
      if (cqlType == DataTypes.DATE) {
        return new DateToTemporalCodec<>(Date.class, TypeCodecs.DATE, timeZone);
      }
      if (cqlType == DataTypes.TIME) {
        return new DateToTemporalCodec<>(Date.class, TypeCodecs.TIME, timeZone);
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        return new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, timeZone);
      }
    }
    if (javaType.isSubtypeOf(dateGenericType) && isUUID(cqlType)) {
      @SuppressWarnings("unchecked")
      TypeCodec<UUID> uuidCodec = (TypeCodec<UUID>) codecFor(cqlType);
      @SuppressWarnings("unchecked")
      DateToTemporalCodec<Date, Instant> instantCodec =
          (DateToTemporalCodec<Date, Instant>)
              maybeCreateConvertingCodec(DataTypes.TIMESTAMP, javaType);
      assert instantCodec != null;
      return new DateToUUIDCodec<>(uuidCodec, instantCodec, generator);
    }
    if (javaType.isSubtypeOf(GenericType.BOOLEAN) && isNumeric(cqlType)) {
      @SuppressWarnings("unchecked")
      TypeCodec<Number> typeCodec = (TypeCodec<Number>) codecFor(cqlType);
      return new BooleanToNumberCodec<>(typeCodec, booleanNumbers);
    }
    return null;
  }

  private ConvertingCodec<String, ?> createStringConvertingCodec(
      @NotNull DataType cqlType, boolean rootCodec) {
    // Don't apply null strings for non-root codecs
    List<String> nullStrings = rootCodec ? this.nullStrings : ImmutableList.of();
    // DataType.Name name = cqlType.getName();
    int cqlTypeCode = cqlType.getProtocolCode();
    switch (cqlTypeCode) {
      case ASCII:
      case VARCHAR:
        @SuppressWarnings("unchecked")
        TypeCodec<String> typeCodec = (TypeCodec<String>) codecFor(cqlType);
        return new StringToStringCodec(typeCodec, nullStrings);
      case BOOLEAN:
        return new StringToBooleanCodec(booleanInputWords, booleanOutputWords, nullStrings);
      case TINYINT:
        return new StringToByteCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case SMALLINT:
        return new StringToShortCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case INT:
        return new StringToIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case BIGINT:
        return new StringToLongCodec(
            TypeCodecs.BIGINT,
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case COUNTER:
        return new StringToLongCodec(
            TypeCodecs.COUNTER,
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case FLOAT:
        return new StringToFloatCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DOUBLE:
        return new StringToDoubleCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case VARINT:
        return new StringToBigIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DECIMAL:
        return new StringToBigDecimalCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DATE:
        return new StringToLocalDateCodec(localDateFormat, timeZone, nullStrings);
      case TIME:
        return new StringToLocalTimeCodec(localTimeFormat, timeZone, nullStrings);
      case TIMESTAMP:
        return new StringToInstantCodec(
            timestampFormat, numberFormat, timeZone, timeUnit, epoch, nullStrings);
      case INET:
        return new StringToInetAddressCodec(nullStrings);
      case UUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>)
                  createStringConvertingCodec(DataTypes.TIMESTAMP, false);
          return new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, generator, nullStrings);
        }
      case TIMEUUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>)
                  createStringConvertingCodec(DataTypes.TIMESTAMP, false);
          return new StringToUUIDCodec(TypeCodecs.TIMEUUID, instantCodec, generator, nullStrings);
        }
      case BLOB:
        return new StringToBlobCodec(nullStrings);
      case DURATION:
        return new StringToDurationCodec(nullStrings);
      case LIST:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToListCodec<Object> jsonCodec =
              (JsonNodeToListCodec<Object>) createJsonNodeConvertingCodec(cqlType, false);
          return new StringToListCodec<>(jsonCodec, objectMapper, nullStrings);
        }
      case SET:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToSetCodec<Object> jsonCodec =
              (JsonNodeToSetCodec<Object>) createJsonNodeConvertingCodec(cqlType, false);
          return new StringToSetCodec<>(jsonCodec, objectMapper, nullStrings);
        }
      case MAP:
        {
          @SuppressWarnings("unchecked")
          JsonNodeToMapCodec<Object, Object> jsonCodec =
              (JsonNodeToMapCodec<Object, Object>) createJsonNodeConvertingCodec(cqlType, false);
          return new StringToMapCodec<>(jsonCodec, objectMapper, nullStrings);
        }
      case TUPLE:
        {
          JsonNodeToTupleCodec jsonCodec =
              (JsonNodeToTupleCodec) createJsonNodeConvertingCodec(cqlType, false);
          return new StringToTupleCodec(jsonCodec, objectMapper, nullStrings);
        }
      case UDT:
        {
          JsonNodeToUDTCodec jsonCodec =
              (JsonNodeToUDTCodec) createJsonNodeConvertingCodec(cqlType, false);
          return new StringToUDTCodec(jsonCodec, objectMapper, nullStrings);
        }
      case CUSTOM:
        {
          CustomType customType = (CustomType) cqlType;
          switch (customType.getClassName()) {
            case PointCodec.CLASS_NAME:
              return new StringToPointCodec(nullStrings);
            case LineStringCodec.CLASS_NAME:
              return new StringToLineStringCodec(nullStrings);
            case PolygonCodec.CLASS_NAME:
              return new StringToPolygonCodec(nullStrings);
            case DATE_RANGE_CLASS_NAME:
              return new StringToDateRangeCodec(nullStrings);
          }
          // fall through
        }
      default:
        try {
          TypeCodec<?> innerCodec = codecFor(cqlType);
          LOGGER.warn(
              String.format(
                  "CQL type %s is not officially supported by this version of DSBulk; "
                      + "string literals will be parsed and formatted using registered codec %s",
                  cqlType, innerCodec.getClass().getSimpleName()));
          return new StringToUnknownTypeCodec<>(innerCodec, nullStrings);
        } catch (CodecNotFoundException e) {
          String msg =
              String.format(
                  "Codec not found for requested operation: [%s <-> %s]", cqlType, String.class);
          CodecNotFoundException e1 =
              new CodecNotFoundException(new RuntimeException(msg, e), cqlType, GenericType.STRING);
          e1.addSuppressed(e);
          throw e1;
        }
    }
  }

  private ConvertingCodec<JsonNode, ?> createJsonNodeConvertingCodec(
      @NotNull DataType cqlType, boolean rootCodec) {
    // Don't apply null strings for non-root codecs
    List<String> nullStrings = rootCodec ? this.nullStrings : ImmutableList.of();
    //    DataType.Name name = cqlType.getName();
    int cqlTypeCode = cqlType.getProtocolCode();
    switch (cqlTypeCode) {
      case ASCII:
      case VARCHAR:
        @SuppressWarnings("unchecked")
        TypeCodec<String> typeCodec = (TypeCodec<String>) codecFor(cqlType);
        return new JsonNodeToStringCodec(typeCodec, objectMapper, nullStrings);
      case BOOLEAN:
        return new JsonNodeToBooleanCodec(booleanInputWords, nullStrings);
      case TINYINT:
        return new JsonNodeToByteCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case SMALLINT:
        return new JsonNodeToShortCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case INT:
        return new JsonNodeToIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case BIGINT:
        return new JsonNodeToLongCodec(
            TypeCodecs.BIGINT,
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case COUNTER:
        return new JsonNodeToLongCodec(
            TypeCodecs.COUNTER,
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case FLOAT:
        return new JsonNodeToFloatCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DOUBLE:
        return new JsonNodeToDoubleCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case VARINT:
        return new JsonNodeToBigIntegerCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DECIMAL:
        return new JsonNodeToBigDecimalCodec(
            numberFormat,
            overflowStrategy,
            roundingMode,
            timestampFormat,
            timeZone,
            timeUnit,
            epoch,
            booleanInputWords,
            booleanNumbers,
            nullStrings);
      case DATE:
        return new JsonNodeToLocalDateCodec(localDateFormat, nullStrings);
      case TIME:
        return new JsonNodeToLocalTimeCodec(localTimeFormat, nullStrings);
      case TIMESTAMP:
        return new JsonNodeToInstantCodec(
            timestampFormat, numberFormat, timeZone, timeUnit, epoch, nullStrings);
      case INET:
        return new JsonNodeToInetAddressCodec(nullStrings);
      case UUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>)
                  createStringConvertingCodec(DataTypes.TIMESTAMP, false);
          return new JsonNodeToUUIDCodec(TypeCodecs.UUID, instantCodec, generator, nullStrings);
        }
      case TIMEUUID:
        {
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Instant> instantCodec =
              (ConvertingCodec<String, Instant>)
                  createStringConvertingCodec(DataTypes.TIMESTAMP, false);
          return new JsonNodeToUUIDCodec(TypeCodecs.TIMEUUID, instantCodec, generator, nullStrings);
        }
      case BLOB:
        return new JsonNodeToBlobCodec(nullStrings);
      case DURATION:
        return new JsonNodeToDurationCodec(nullStrings);
      case LIST:
        {
          DataType elementType = ((ListType) cqlType).getElementType();
          @SuppressWarnings("unchecked")
          TypeCodec<List<Object>> collectionCodec = (TypeCodec<List<Object>>) codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType, false);
          return new JsonNodeToListCodec<>(collectionCodec, eltCodec, objectMapper, nullStrings);
        }
      case SET:
        {
          DataType elementType = ((SetType) cqlType).getElementType();
          @SuppressWarnings("unchecked")
          TypeCodec<Set<Object>> collectionCodec = (TypeCodec<Set<Object>>) codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(elementType, false);
          return new JsonNodeToSetCodec<>(collectionCodec, eltCodec, objectMapper, nullStrings);
        }
      case MAP:
        {
          DataType keyType = ((MapType) cqlType).getKeyType();
          DataType valueType = ((MapType) cqlType).getValueType();
          @SuppressWarnings("unchecked")
          TypeCodec<Map<Object, Object>> mapCodec =
              (TypeCodec<Map<Object, Object>>) codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<String, Object> keyCodec =
              (ConvertingCodec<String, Object>) createStringConvertingCodec(keyType, false);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> valueCodec =
              (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(valueType, false);
          return new JsonNodeToMapCodec<>(
              mapCodec, keyCodec, valueCodec, objectMapper, nullStrings);
        }
      case TUPLE:
        {
          @SuppressWarnings("unchecked")
          TypeCodec<TupleValue> tupleCodec = (TypeCodec<TupleValue>) codecFor(cqlType);
          ImmutableList.Builder<ConvertingCodec<JsonNode, Object>> eltCodecs =
              new ImmutableList.Builder<>();
          for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
            @SuppressWarnings("unchecked")
            ConvertingCodec<JsonNode, Object> eltCodec =
                (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(eltType, false);
            eltCodecs.add(eltCodec);
          }
          return new JsonNodeToTupleCodec(tupleCodec, eltCodecs.build(), objectMapper, nullStrings);
        }
      case UDT:
        {
          @SuppressWarnings("unchecked")
          TypeCodec<UdtValue> udtCodec = (TypeCodec<UdtValue>) codecFor(cqlType);
          ImmutableMap.Builder<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs =
              new ImmutableMap.Builder<>();
          List<CqlIdentifier> fieldNames = ((UserDefinedType) cqlType).getFieldNames();
          List<DataType> fieldTypes = ((UserDefinedType) cqlType).getFieldTypes();
          assert (fieldNames.size() == fieldTypes.size());

          for (int idx = 0; idx < fieldNames.size(); idx++) {
            CqlIdentifier fieldName = fieldNames.get(idx);
            DataType fieldType = fieldTypes.get(idx);
            @SuppressWarnings("unchecked")
            ConvertingCodec<JsonNode, Object> fieldCodec =
                (ConvertingCodec<JsonNode, Object>) createJsonNodeConvertingCodec(fieldType, false);
            fieldCodecs.put(fieldName, fieldCodec);
          }
          return new JsonNodeToUDTCodec(udtCodec, fieldCodecs.build(), objectMapper, nullStrings);
        }
      case CUSTOM:
        {
          CustomType customType = (CustomType) cqlType;
          switch (customType.getClassName()) {
            case PointCodec.CLASS_NAME:
              return new JsonNodeToPointCodec(objectMapper, nullStrings);
            case LineStringCodec.CLASS_NAME:
              return new JsonNodeToLineStringCodec(objectMapper, nullStrings);
            case PolygonCodec.CLASS_NAME:
              return new JsonNodeToPolygonCodec(objectMapper, nullStrings);
            case DATE_RANGE_CLASS_NAME:
              return new JsonNodeToDateRangeCodec(nullStrings);
          }
          // fall through
        }
      default:
        try {
          TypeCodec<?> innerCodec = codecFor(cqlType);
          LOGGER.warn(
              String.format(
                  "CQL type %s is not officially supported by this version of DSBulk; "
                      + "JSON literals will be parsed and formatted using registered codec %s",
                  cqlType, innerCodec.getClass().getSimpleName()));
          return new JsonNodeToUnknownTypeCodec<>(innerCodec, nullStrings);
        } catch (CodecNotFoundException e) {
          String msg =
              String.format(
                  "Codec not found for requested operation: [%s <-> %s]", cqlType, JsonNode.class);
          CodecNotFoundException e1 =
              new CodecNotFoundException(
                  new RuntimeException(msg, e), cqlType, GenericType.of(JsonNode.class));
          e1.addSuppressed(e);
          throw e1;
        }
    }
  }

  // DAT-288: avoid returning legacy temporal codecs or collection codecs whose elements are legacy
  // temporal codecs.
  public @NotNull TypeCodec<?> codecFor(@NotNull DataType cqlType) {
    int protocolCode = cqlType.getProtocolCode();
    switch (protocolCode) {
      case TIMESTAMP:
        return TypeCodecs.TIMESTAMP;
      case DATE:
        return TypeCodecs.DATE;
      case TIME:
        return TypeCodecs.TIME;
      case LIST:
        return TypeCodecs.listOf(codecFor(((ListType) cqlType).getElementType()));
      case SET:
        return TypeCodecs.setOf(codecFor(((SetType) cqlType).getElementType()));
      case MAP:
        return TypeCodecs.mapOf(
            codecFor(((MapType) cqlType).getKeyType()),
            codecFor(((MapType) cqlType).getValueType()));
      default:
        return codecRegistry.codecFor(cqlType);
    }
  }

  private static boolean isNumeric(@NotNull DataType cqlType) {
    return cqlType == DataTypes.TINYINT
        || cqlType == DataTypes.SMALLINT
        || cqlType == DataTypes.INT
        || cqlType == DataTypes.BIGINT
        || cqlType == DataTypes.FLOAT
        || cqlType == DataTypes.DOUBLE
        || cqlType == DataTypes.VARINT
        || cqlType == DataTypes.DECIMAL;
  }

  private static boolean isCollection(@NotNull DataType cqlType) {
    return cqlType instanceof SetType || cqlType instanceof ListType;
  }

  private static boolean isTemporal(@NotNull DataType cqlType) {
    return cqlType == DataTypes.DATE || cqlType == DataTypes.TIME || cqlType == DataTypes.TIMESTAMP;
  }

  private static boolean isUUID(@NotNull DataType cqlType) {
    return cqlType == DataTypes.UUID || cqlType == DataTypes.TIMEUUID;
  }
}
