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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonNodeToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();

  private final JsonNodeToBooleanCodec codec =
      new JsonNodeToBooleanCodec(inputs, Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.booleanNode(true))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.booleanNode(false))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FOO"))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("BAR"))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("foo"))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("bar"))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(true)
        .toExternal(JSON_NODE_FACTORY.booleanNode(true))
        .convertsFromInternal(false)
        .toExternal(JSON_NODE_FACTORY.booleanNode(false))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid boolean"));
  }
}
