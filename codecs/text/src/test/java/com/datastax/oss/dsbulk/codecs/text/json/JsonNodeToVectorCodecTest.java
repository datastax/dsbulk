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

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CqlVectorCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JsonNodeToVectorCodecTest {
  private final ArrayList<Float> values = Lists.newArrayList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);
  private final CqlVector vector = CqlVector.builder().addAll(values).build();
  private final CqlVectorCodec vectorCodec =
      new CqlVectorCodec(new CqlVectorType(DataTypes.FLOAT, 5), TypeCodecs.FLOAT);
  private final ArrayNode vectorDoc;

  private final ConvertingCodecFactory factory = new ConvertingCodecFactory();
  private final JsonNodeConvertingCodecProvider provider = new JsonNodeConvertingCodecProvider();
  private final JsonNodeToVectorCodec dsbulkCodec =
      new JsonNodeToVectorCodec(
          vectorCodec,
          provider
              .maybeProvide(DataTypes.FLOAT, GenericType.of(JsonNode.class), factory, false)
              .get(),
          JsonCodecUtils.getObjectMapper(),
          Lists.newArrayList("NULL"));

  public JsonNodeToVectorCodecTest() {
    this.vectorDoc = JSON_NODE_FACTORY.arrayNode();
    for (float value : values) {
      this.vectorDoc.add(JSON_NODE_FACTORY.numberNode(value));
    }
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(dsbulkCodec)
        .convertsFromExternal(vectorDoc) // standard pattern
        .toInternal(vector)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(dsbulkCodec)
        .convertsFromInternal(vector)
        .toExternal(vectorDoc)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  @Disabled("Requires driver support for validating a CqlVector against a CqlVectorType")
  void should_not_convert_from_invalid_internal() {
    // Too few values to match dimensions
    ArrayList<Float> tooMany = Lists.newArrayList(values);
    tooMany.add(6.6f);
    ArrayList<Float> tooFew = Lists.newArrayList(values);
    tooFew.remove(0);

    assertThat(dsbulkCodec)
        .cannotConvertFromInternal(CqlVector.builder().addAll(tooMany).build())
        .cannotConvertFromInternal(CqlVector.builder().addAll(tooFew).build())
        .cannotConvertFromInternal("not a valid vector");
  }
}
