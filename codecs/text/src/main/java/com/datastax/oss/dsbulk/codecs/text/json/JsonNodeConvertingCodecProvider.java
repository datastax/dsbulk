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
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.ALLOW_EXTRA_FIELDS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.ALLOW_MISSING_FIELDS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.BOOLEAN_INPUT_WORDS;
import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.BOOLEAN_NUMBERS;
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
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import com.datastax.oss.dsbulk.codecs.text.json.dse.JsonNodeToDateRangeCodec;
import com.datastax.oss.dsbulk.codecs.text.json.dse.JsonNodeToLineStringCodec;
import com.datastax.oss.dsbulk.codecs.text.json.dse.JsonNodeToPointCodec;
import com.datastax.oss.dsbulk.codecs.text.json.dse.JsonNodeToPolygonCodec;
import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized {@link DefaultCodecRegistry} that is capable of producing {@link ConvertingCodec}s.
 */
public class JsonNodeConvertingCodecProvider implements ConvertingCodecProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JsonNodeConvertingCodecProvider.class);

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
    if (!externalJavaType.equals(JSON_NODE_TYPE)) {
      return Optional.empty();
    }
    ConvertingCodec<JsonNode, ?> codec =
        createJsonNodeConvertingCodec(cqlType, codecFactory, rootCodec);
    return Optional.ofNullable(codec);
  }

  @Nullable
  private ConvertingCodec<JsonNode, ?> createJsonNodeConvertingCodec(
      @NonNull DataType cqlType, @NonNull ConvertingCodecFactory codecFactory, boolean rootCodec) {
    CodecRegistry codecRegistry = codecFactory.getCodecRegistry();
    ConversionContext context = codecFactory.getContext();
    // Don't apply null strings for non-root codecs
    List<String> nullStrings = rootCodec ? context.getAttribute(NULL_STRINGS) : ImmutableList.of();
    int cqlTypeCode = cqlType.getProtocolCode();
    switch (cqlTypeCode) {
      case ASCII:
      case VARCHAR:
        TypeCodec<String> typeCodec = codecRegistry.codecFor(cqlType);
        return new JsonNodeToStringCodec(
            typeCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
      case BOOLEAN:
        return new JsonNodeToBooleanCodec(context.getAttribute(BOOLEAN_INPUT_WORDS), nullStrings);
      case TINYINT:
        return new JsonNodeToByteCodec(
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
        return new JsonNodeToShortCodec(
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
        return new JsonNodeToIntegerCodec(
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
        return new JsonNodeToLongCodec(
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
        return new JsonNodeToLongCodec(
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
        return new JsonNodeToFloatCodec(
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
        return new JsonNodeToDoubleCodec(
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
        return new JsonNodeToBigIntegerCodec(
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
        return new JsonNodeToBigDecimalCodec(
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
        return new JsonNodeToLocalDateCodec(
            context.getAttribute(LOCAL_DATE_FORMAT), context.getAttribute(TIME_ZONE), nullStrings);
      case TIME:
        return new JsonNodeToLocalTimeCodec(
            context.getAttribute(LOCAL_TIME_FORMAT), context.getAttribute(TIME_ZONE), nullStrings);
      case TIMESTAMP:
        return new JsonNodeToInstantCodec(
            context.getAttribute(TIMESTAMP_FORMAT),
            context.getAttribute(TIME_ZONE),
            context.getAttribute(EPOCH),
            nullStrings);
      case INET:
        return new JsonNodeToInetAddressCodec(nullStrings);
      case UUID:
        {
          ConvertingCodec<String, Instant> instantCodec =
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, false);
          return new JsonNodeToUUIDCodec(
              TypeCodecs.UUID,
              instantCodec,
              context.getAttribute(TIME_UUID_GENERATOR),
              nullStrings);
        }
      case TIMEUUID:
        {
          ConvertingCodec<String, Instant> instantCodec =
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, false);
          return new JsonNodeToUUIDCodec(
              TypeCodecs.TIMEUUID,
              instantCodec,
              context.getAttribute(TIME_UUID_GENERATOR),
              nullStrings);
        }
      case BLOB:
        return new JsonNodeToBlobCodec(nullStrings);
      case DURATION:
        return new JsonNodeToDurationCodec(nullStrings);
      case LIST:
        {
          DataType elementType = ((ListType) cqlType).getElementType();
          TypeCodec<List<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>)
                  createJsonNodeConvertingCodec(elementType, codecFactory, false);
          return new JsonNodeToListCodec<>(
              collectionCodec, eltCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case SET:
        {
          DataType elementType = ((SetType) cqlType).getElementType();
          TypeCodec<Set<Object>> collectionCodec = codecRegistry.codecFor(cqlType);
          @SuppressWarnings("unchecked")
          ConvertingCodec<JsonNode, Object> eltCodec =
              (ConvertingCodec<JsonNode, Object>)
                  createJsonNodeConvertingCodec(elementType, codecFactory, false);
          return new JsonNodeToSetCodec<>(
              collectionCodec, eltCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case MAP:
        {
          DataType keyType = ((MapType) cqlType).getKeyType();
          DataType valueType = ((MapType) cqlType).getValueType();
          TypeCodec<Map<Object, Object>> mapCodec = codecRegistry.codecFor(cqlType);
          ConvertingCodec<String, Object> keyCodec =
              codecFactory.createConvertingCodec(keyType, GenericType.STRING, false);
          ConvertingCodec<JsonNode, Object> valueCodec =
              codecFactory.createConvertingCodec(valueType, JSON_NODE_TYPE, false);
          return new JsonNodeToMapCodec<>(
              mapCodec, keyCodec, valueCodec, context.getAttribute(OBJECT_MAPPER), nullStrings);
        }
      case TUPLE:
        {
          TypeCodec<TupleValue> tupleCodec = codecRegistry.codecFor(cqlType);
          ImmutableList.Builder<ConvertingCodec<JsonNode, Object>> eltCodecs =
              new ImmutableList.Builder<>();
          for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
            ConvertingCodec<JsonNode, Object> eltCodec =
                codecFactory.createConvertingCodec(eltType, JSON_NODE_TYPE, false);
            eltCodecs.add(Objects.requireNonNull(eltCodec));
          }
          return new JsonNodeToTupleCodec(
              tupleCodec,
              eltCodecs.build(),
              context.getAttribute(OBJECT_MAPPER),
              nullStrings,
              context.getAttribute(ALLOW_EXTRA_FIELDS),
              context.getAttribute(ALLOW_MISSING_FIELDS));
        }
      case UDT:
        {
          TypeCodec<UdtValue> udtCodec = codecRegistry.codecFor(cqlType);
          ImmutableMap.Builder<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs =
              new ImmutableMap.Builder<>();
          List<CqlIdentifier> fieldNames = ((UserDefinedType) cqlType).getFieldNames();
          List<DataType> fieldTypes = ((UserDefinedType) cqlType).getFieldTypes();
          assert (fieldNames.size() == fieldTypes.size());

          for (int idx = 0; idx < fieldNames.size(); idx++) {
            CqlIdentifier fieldName = fieldNames.get(idx);
            DataType fieldType = fieldTypes.get(idx);
            ConvertingCodec<JsonNode, Object> fieldCodec =
                codecFactory.createConvertingCodec(fieldType, JSON_NODE_TYPE, false);
            fieldCodecs.put(fieldName, Objects.requireNonNull(fieldCodec));
          }
          return new JsonNodeToUDTCodec(
              udtCodec,
              fieldCodecs.build(),
              context.getAttribute(OBJECT_MAPPER),
              nullStrings,
              context.getAttribute(ALLOW_EXTRA_FIELDS),
              context.getAttribute(ALLOW_MISSING_FIELDS));
        }
      case CUSTOM:
        {
          CustomType customType = (CustomType) cqlType;
          switch (customType.getClassName()) {
            case POINT_CLASS_NAME:
              return new JsonNodeToPointCodec(context.getAttribute(OBJECT_MAPPER), nullStrings);
            case LINE_STRING_CLASS_NAME:
              return new JsonNodeToLineStringCodec(
                  context.getAttribute(OBJECT_MAPPER), nullStrings);
            case POLYGON_CLASS_NAME:
              return new JsonNodeToPolygonCodec(context.getAttribute(OBJECT_MAPPER), nullStrings);
            case DATE_RANGE_CLASS_NAME:
              return new JsonNodeToDateRangeCodec(nullStrings);
          }
          // fall through
        }
      default:
        try {
          TypeCodec<?> innerCodec = codecRegistry.codecFor(cqlType);
          LOGGER.warn(
              String.format(
                  "CQL type %s is not officially supported by this version of DSBulk; "
                      + "JSON literals will be parsed and formatted using registered codec %s",
                  cqlType, innerCodec.getClass().getSimpleName()));
          return new JsonNodeToUnknownTypeCodec<>(innerCodec, nullStrings);
        } catch (CodecNotFoundException ignored) {
        }
        return null;
    }
  }
}
