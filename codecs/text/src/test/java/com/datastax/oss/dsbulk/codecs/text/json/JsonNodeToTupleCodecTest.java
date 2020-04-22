/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.JSON_NODE_TYPE;
import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.dsbulk.tests.driver.DriverUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToTupleCodecTest {

  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();

  private TupleType tupleType;

  private JsonNodeToTupleCodec codec1;
  private JsonNodeToTupleCodec codec2;
  private JsonNodeToTupleCodec codec3;

  @BeforeEach
  void setUp() {
    tupleType =
        DriverUtils.mockTupleType(
            DefaultProtocolVersion.V4, CodecRegistry.DEFAULT, DataTypes.TIMESTAMP, DataTypes.TEXT);
    ConversionContext context1 = new TextConversionContext().setNullStrings("NULL", "");
    ConversionContext context2 = new TextConversionContext().setAllowExtraFields(true);
    ConversionContext context3 = new TextConversionContext().setAllowMissingFields(true);
    codec1 =
        (JsonNodeToTupleCodec)
            new ConvertingCodecFactory(context1)
                .<JsonNode, TupleValue>createConvertingCodec(tupleType, JSON_NODE_TYPE, true);
    codec2 =
        (JsonNodeToTupleCodec)
            new ConvertingCodecFactory(context2)
                .<JsonNode, TupleValue>createConvertingCodec(tupleType, JSON_NODE_TYPE, true);
    codec3 =
        (JsonNodeToTupleCodec)
            new ConvertingCodecFactory(context3)
                .<JsonNode, TupleValue>createConvertingCodec(tupleType, JSON_NODE_TYPE, true);
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\",\"+01:00\"]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("['2016-07-24T20:34:12.999','+01:00']"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[,\"\"]"))
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal(objectMapper.readTree("[,\"NULL\"]"))
        .toInternal(tupleType.newValue(null, "NULL"))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    // should allow extra elements
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\",\"+01:00\", 42]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[,\"\",\"\"]"))
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal(objectMapper.readTree("[null,null,null]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(objectMapper.readTree("[,,]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    // should allow missing elements
    assertThat(codec3)
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\"]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), null))
        .convertsFromExternal(objectMapper.readTree("[null]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(
            tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .toExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .convertsFromInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), ""))
        .toExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"\"]"))
        .convertsFromInternal(tupleType.newValue(null, ""))
        .toExternal(objectMapper.readTree("[null,\"\"]"))
        .convertsFromInternal(tupleType.newValue(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid tuple\":42}"));
    // should not allow missing elements
    assertThat(codec2)
        .cannotConvertFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\"]"))
        .cannotConvertFromExternal(objectMapper.readTree("[]"));
    // should not allow extra elements
    assertThat(codec3)
        .cannotConvertFromExternal(
            objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\",42]"));
    // tests for error messages
    assertThatThrownBy(
            () ->
                codec1.externalToInternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\"]")))
        .isInstanceOf(JsonSchemaMismatchException.class)
        .hasMessageContaining("expecting 2 elements, got 1");
    assertThatThrownBy(
            () ->
                codec1.externalToInternal(
                    objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\",42]")))
        .isInstanceOf(JsonSchemaMismatchException.class)
        .hasMessageContaining("expecting 2 elements, got 3");
  }
}
