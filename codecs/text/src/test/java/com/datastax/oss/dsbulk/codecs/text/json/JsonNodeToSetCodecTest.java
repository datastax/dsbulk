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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToSetCodecTest {

  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();

  private JsonNodeToSetCodec<Double> codec1;
  private JsonNodeToSetCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec1 =
        (JsonNodeToSetCodec<Double>)
            codecFactory.<JsonNode, Set<Double>>createConvertingCodec(
                DataTypes.setOf(DataTypes.DOUBLE), JSON_NODE_TYPE, true);
    codec2 =
        (JsonNodeToSetCodec<String>)
            codecFactory.<JsonNode, Set<String>>createConvertingCodec(
                DataTypes.setOf(DataTypes.TEXT), JSON_NODE_TYPE, true);
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .toInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree("[1234.56,78900]"))
        .toInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .toInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(Sets.newLinkedHashSet(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(Sets.newLinkedHashSet())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("['foo','bar']"))
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .toInternal(Sets.newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFromExternal(
            objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .toInternal(Sets.newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(Sets.newLinkedHashSet(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(Sets.newLinkedHashSet(null, null))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(Sets.newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("['','']"))
        .toInternal(Sets.newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(Sets.newLinkedHashSet("NULL"))
        .convertsFromExternal(objectMapper.readTree("['NULL','NULL']"))
        .toInternal(Sets.newLinkedHashSet("NULL"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(Sets.newLinkedHashSet())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .toExternal(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsFromInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .toExternal(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsFromInternal(Sets.newLinkedHashSet(1d, null))
        .toExternal(objectMapper.readTree("[1.0,null]"))
        .convertsFromInternal(Sets.newLinkedHashSet(null, 0d))
        .toExternal(objectMapper.readTree("[null,0.0]"))
        .convertsFromInternal(Sets.newLinkedHashSet((Double) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(Sets.newLinkedHashSet())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codec2)
        .convertsFromInternal(Sets.newLinkedHashSet("foo", "bar"))
        .toExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsFromInternal(Sets.newLinkedHashSet("\"foo\"", "\"bar\""))
        .toExternal(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsFromInternal(Sets.newLinkedHashSet("\\foo\\", "\\bar\\"))
        .toExternal(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsFromInternal(Sets.newLinkedHashSet(",foo,", ",bar,"))
        .toExternal(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsFromInternal(Sets.newLinkedHashSet(""))
        .toExternal(objectMapper.readTree("[\"\"]"))
        .convertsFromInternal(Sets.newLinkedHashSet((String) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(Sets.newLinkedHashSet())
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
