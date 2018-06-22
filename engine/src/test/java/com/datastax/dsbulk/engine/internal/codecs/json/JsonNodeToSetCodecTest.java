/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToSetCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private JsonNodeToSetCodec<Double> codec1;
  private JsonNodeToSetCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry = newCodecRegistry("nullStrings = [NULL]");
    codec1 =
        (JsonNodeToSetCodec<Double>)
            codecRegistry.codecFor(
                DataTypes.setOf(DataTypes.DOUBLE), GenericType.of(JsonNode.class));
    codec2 =
        (JsonNodeToSetCodec<String>)
            codecRegistry.codecFor(DataTypes.setOf(DataTypes.TEXT), GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree("[1234.56,78900]"))
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(newLinkedHashSet())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("['foo','bar']"))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .toInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFromExternal(
            objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .toInternal(newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(newLinkedHashSet(null, null))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("['','']"))
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(newLinkedHashSet((String) "NULL"))
        .convertsFromExternal(objectMapper.readTree("['NULL','NULL']"))
        .toInternal(newLinkedHashSet((String) "NULL"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(newLinkedHashSet())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(newLinkedHashSet(1d, 2d, 3d))
        .toExternal(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsFromInternal(newLinkedHashSet(1234.56d, 78900d))
        .toExternal(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsFromInternal(newLinkedHashSet(1d, null))
        .toExternal(objectMapper.readTree("[1.0,null]"))
        .convertsFromInternal(newLinkedHashSet(null, 0d))
        .toExternal(objectMapper.readTree("[null,0.0]"))
        .convertsFromInternal(newLinkedHashSet((Double) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(newLinkedHashSet())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codec2)
        .convertsFromInternal(newLinkedHashSet("foo", "bar"))
        .toExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsFromInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .toExternal(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsFromInternal(newLinkedHashSet("\\foo\\", "\\bar\\"))
        .toExternal(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsFromInternal(newLinkedHashSet(",foo,", ",bar,"))
        .toExternal(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsFromInternal(newLinkedHashSet(""))
        .toExternal(objectMapper.readTree("[\"\"]"))
        .convertsFromInternal(newLinkedHashSet((String) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(newLinkedHashSet())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid double\"]"))
        .cannotConvertFromExternal(objectMapper.readTree("{ \"not a valid array\" : 42 }"))
        .cannotConvertFromExternal(objectMapper.readTree("42"));
  }
}
