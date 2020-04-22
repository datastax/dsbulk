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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToListCodecTest {

  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();

  private JsonNodeToListCodec<Double> codec1;

  private JsonNodeToListCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec1 =
        (JsonNodeToListCodec<Double>)
            codecFactory.<JsonNode, List<Double>>createConvertingCodec(
                DataTypes.listOf(DataTypes.DOUBLE), JSON_NODE_TYPE, true);
    codec2 =
        (JsonNodeToListCodec<String>)
            codecFactory.<JsonNode, List<String>>createConvertingCodec(
                DataTypes.listOf(DataTypes.TEXT), JSON_NODE_TYPE, true);
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal(Lists.newArrayList(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .toInternal(Lists.newArrayList(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree("[1234.56,78900]"))
        .toInternal(Lists.newArrayList(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .toInternal(Lists.newArrayList(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(ImmutableList.of())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("['foo','bar']"))
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .toInternal(Lists.newArrayList("\"foo\"", "\"bar\""))
        .convertsFromExternal(
            objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .toInternal(Lists.newArrayList("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(Lists.newArrayList(null, null))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(Lists.newArrayList("", ""))
        .convertsFromExternal(objectMapper.readTree("['','']"))
        .toInternal(Lists.newArrayList("", ""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(Lists.newArrayList("NULL", "NULL"))
        .convertsFromExternal(objectMapper.readTree("['NULL','NULL']"))
        .toInternal(Lists.newArrayList("NULL", "NULL"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(ImmutableList.of())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(Lists.newArrayList(1d, 2d, 3d))
        .toExternal(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsFromInternal(Lists.newArrayList(1234.56d, 78900d))
        .toExternal(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsFromInternal(Lists.newArrayList(1d, null))
        .toExternal(objectMapper.readTree("[1.0,null]"))
        .convertsFromInternal(Lists.newArrayList(null, 0d))
        .toExternal(objectMapper.readTree("[null,0.0]"))
        .convertsFromInternal(Lists.newArrayList(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(ImmutableList.of())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codec2)
        .convertsFromInternal(Lists.newArrayList("foo", "bar"))
        .toExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsFromInternal(Lists.newArrayList("\"foo\"", "\"bar\""))
        .toExternal(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsFromInternal(Lists.newArrayList("\\foo\\", "\\bar\\"))
        .toExternal(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsFromInternal(Lists.newArrayList(",foo,", ",bar,"))
        .toExternal(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsFromInternal(Lists.newArrayList("", ""))
        .toExternal(objectMapper.readTree("[\"\",\"\"]"))
        .convertsFromInternal(Lists.newArrayList(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(ImmutableList.of())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid double\"]"));
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("{ \"not a valid array\" : 42 }"));
    assertThat(codec1).cannotConvertFromExternal(objectMapper.readTree("42"));
  }
}
