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
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.BINARY_FORMAT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.BOOLEAN_INPUT_WORDS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.BOOLEAN_NUMBERS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.BOOLEAN_OUTPUT_WORDS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.EPOCH;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.LOCAL_DATE_FORMAT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.LOCAL_TIME_FORMAT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.NULL_STRINGS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.NUMBER_FORMAT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.OBJECT_MAPPER;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.OVERFLOW_STRATEGY;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.ROUNDING_MODE;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.TIMESTAMP_FORMAT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.TIME_UNIT;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.TIME_UUID_GENERATOR;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.TIME_ZONE;
import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_TYPE;
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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import com.datastax.oss.dsbulk.codecs.text.string.dse.StringToDateRangeCodec;
import com.datastax.oss.dsbulk.codecs.text.string.dse.StringToLineStringCodec;
import com.datastax.oss.dsbulk.codecs.text.string.dse.StringToPointCodec;
import com.datastax.oss.dsbulk.codecs.text.string.dse.StringToPolygonCodec;
import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized {@link DefaultCodecRegistry} that is capable of producing {@link ConvertingCodec}s.
 */
public class StringConvertingCodecProvider implements ConvertingCodecProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringConvertingCodecProvider.class);

  private static final String LINE_STRING_CLASS_NAME =
      "org.apache.cassandra.db.marshal.LineStringType";
  private static final String POINT_CLASS_NAME = "org.apache.cassandra.db.marshal.PointType";
  private static final String POLYGON_CLASS_NAME = "org.apache.cassandra.db.marshal.PolygonType";
  private static final String DATE_RANGE_CLASS_NAME =
      "org.apache.cassandra.db.marshal.DateRangeType";

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (!externalJavaType.equals(GenericType.STRING)) {
      return Optional.empty();
    }
    ConvertingCodec<String, ?> codec =
        createStringConvertingCodec(cqlType, codecFactory, rootCodec);
    return Optional.ofNullable(codec);
  }

  @Nullable
  private ConvertingCodec<String, ?> createStringConvertingCodec(
      @NonNull DataType cqlType, @NonNull ConvertingCodecFactory codecFactory, boolean rootCodec) {
    ConversionContext context = codecFactory.getContext();
    // DAT-297: Don't apply null strings for non-root codecs
    List<String> nullStrings = rootCodec ? context.getAttribute(NULL_STRINGS) : ImmutableList.of();
    int cqlTypeCode = cqlType.getProtocolCode();
    switch (cqlTypeCode) {
      case ASCII:
      case VARCHAR:
        TypeCodec<String> typeCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
        return new StringToStringCodec(typeCodec, nullStrings);
      case BOOLEAN:
        return new StringToBooleanCodec(
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_OUTPUT_WORDS),
            nullStrings);
      case TINYINT:
        return new StringToByteCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case SMALLINT:
        return new StringToShortCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case INT:
        return new StringToIntegerCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case BIGINT:
        return new StringToLongCodec(
            TypeCodecs.BIGINT,
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case COUNTER:
        return new StringToLongCodec(
            TypeCodecs.COUNTER,
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case FLOAT:
        return new StringToFloatCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case DOUBLE:
        return new StringToDoubleCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case VARINT:
        return new StringToBigIntegerCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case DECIMAL:
        return new StringToBigDecimalCodec(
            context.getAttribute(NUMBER_FORMAT),
            context.getAttribute(OVERFLOW_STRATEGY),
            context.getAttribute(ROUNDING_MODE),
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(TIME_UNIT),
            context.getAttribute(EPOCH),
            context.getAttribute(BOOLEAN_INPUT_WORDS),
            context.getAttribute(BOOLEAN_NUMBERS),
            nullStrings);
      case DATE:
        return new StringToLocalDateCodec(
            context.getAttribute(LOCAL_DATE_FORMAT), context.getAttribute(TIME_ZONE), nullStrings);
      case TIME:
        return new StringToLocalTimeCodec(
            context.getAttribute(LOCAL_TIME_FORMAT), context.getAttribute(TIME_ZONE), nullStrings);
      case TIMESTAMP:
        return new StringToInstantCodec(
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(EPOCH),
            nullStrings);
      case INET:
        return new StringToInetAddressCodec(nullStrings);
      case UUID:
        {
          ConvertingCodec<String, Instant> instantCodec =
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, false);
          return new StringToUUIDCodec(
              TypeCodecs.UUID,
              instantCodec,
              context.getAttribute(TIME_UUID_GENERATOR),
              nullStrings);
        }
      case TIMEUUID:
        {
          ConvertingCodec<String, Instant> instantCodec =
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, false);
          return new StringToUUIDCodec(
              TypeCodecs.TIMEUUID,
              instantCodec,
              context.getAttribute(TIME_UUID_GENERATOR),
              nullStrings);
        }
      case BLOB:
        return new StringToBlobCodec(nullStrings, context.getAttribute(BINARY_FORMAT));
      case DURATION:
        return new StringToDurationCodec(nullStrings);
      case LIST:
        {
          ConvertingCodec<JsonNode, List<Object>> jsonCodec =
              codecFactory.createConvertingCodec(cqlType, JSON_NODE_TYPE, false);
          return new StringToListCodec<>(
              jsonCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case SET:
        {
          ConvertingCodec<JsonNode, Set<Object>> jsonCodec =
              codecFactory.createConvertingCodec(cqlType, JSON_NODE_TYPE, false);
          return new StringToSetCodec<>(
              jsonCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case MAP:
        {
          ConvertingCodec<JsonNode, Map<Object, Object>> jsonCodec =
              codecFactory.createConvertingCodec(cqlType, JSON_NODE_TYPE, false);
          return new StringToMapCodec<>(
              jsonCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case TUPLE:
        {
          ConvertingCodec<JsonNode, TupleValue> jsonCodec =
              codecFactory.createConvertingCodec(cqlType, JSON_NODE_TYPE, false);
          return new StringToTupleCodec(
              jsonCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case UDT:
        {
          ConvertingCodec<JsonNode, UdtValue> jsonCodec =
              codecFactory.createConvertingCodec(cqlType, JSON_NODE_TYPE, false);
          return new StringToUDTCodec(jsonCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case CUSTOM:
        {
          CustomType customType = (CustomType) cqlType;
          switch (customType.getClassName()) {
            case POINT_CLASS_NAME:
              return new StringToPointCodec(nullStrings);
            case LINE_STRING_CLASS_NAME:
              return new StringToLineStringCodec(nullStrings);
            case POLYGON_CLASS_NAME:
              return new StringToPolygonCodec(nullStrings);
            case DATE_RANGE_CLASS_NAME:
              return new StringToDateRangeCodec(nullStrings);
          }
          // fall through
        }
      default:
        try {
          TypeCodec<?> innerCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
          LOGGER.warn(
              String.format(
                  "CQL type %s is not officially supported by this version of DSBulk; "
                      + "string literals will be parsed and formatted using registered codec %s",
                  cqlType, innerCodec.getClass().getSimpleName()));
          return new StringToUnknownTypeCodec<>(innerCodec, nullStrings);
        } catch (CodecNotFoundException ignored) {
        }
        return null;
    }
  }
}
