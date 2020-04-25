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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StringToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();

  private final Map<Boolean, String> outputs =
      ImmutableMap.<Boolean, String>builder().put(true, "foo").put(false, "bar").build();

  private final StringToBooleanCodec codec =
      new StringToBooleanCodec(inputs, outputs, Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("FOO")
        .toInternal(true)
        .convertsFromExternal("BAR")
        .toInternal(false)
        .convertsFromExternal("foo")
        .toInternal(true)
        .convertsFromExternal("bar")
        .toInternal(false)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(true)
        .toExternal("foo")
        .convertsFromInternal(false)
        .toExternal("bar")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid boolean");
  }
}
