/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newTupleType;
import static com.datastax.dsbulk.commons.config.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;

import com.datastax.dsbulk.commons.config.CodecSettings;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToTupleCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private TupleType tupleType;

  private JsonNodeToTupleCodec codec;

  @BeforeEach
  void setUp() {
    tupleType =
        newTupleType(
            //            V4, new CodecRegistry().register(InstantCodec.instance),
            // DataTypes.TIMESTAMP, varchar());
            V4,
            new DefaultCodecRegistry("test", TypeCodecs.TIMESTAMP),
            DataTypes.TIMESTAMP,
            DataTypes.TEXT);
    codec =
        (JsonNodeToTupleCodec)
            newCodecRegistry("nullStrings = [NULL, \"\"]")
                .codecFor(tupleType, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
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
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
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
    assertThat(codec)
        .cannotConvertFromExternal(objectMapper.readTree("[\"not a valid tuple\"]"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid tuple\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\"]"));
  }
}
