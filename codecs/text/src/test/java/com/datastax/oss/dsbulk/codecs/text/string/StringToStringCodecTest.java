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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class StringToStringCodecTest {

  @Test
  void should_convert_from_valid_external() {
    StringToStringCodec codec = new StringToStringCodec(TypeCodecs.TEXT, Collections.emptyList());
    assertThat(codec)
        .convertsFromExternal("foo")
        .toInternal("foo")
        .convertsFromExternal("")
        .toInternal("")
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_external_with_custom_null_string() {
    StringToStringCodec codec =
        new StringToStringCodec(TypeCodecs.TEXT, Lists.newArrayList("NULL", "NADA"));
    assertThat(codec)
        .convertsFromExternal("foo")
        .toInternal("foo")
        .convertsFromExternal("")
        .toInternal("")
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("NADA")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToStringCodec codec = new StringToStringCodec(TypeCodecs.TEXT, Collections.emptyList());
    assertThat(codec)
        .convertsFromInternal("foo")
        .toExternal("foo")
        .convertsFromInternal("")
        .toExternal("")
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_from_valid_internal_with_custom_null_string() {
    StringToStringCodec codec =
        new StringToStringCodec(TypeCodecs.TEXT, Lists.newArrayList("NULL"));
    assertThat(codec)
        .convertsFromInternal("foo")
        .toExternal("foo")
        .convertsFromInternal("")
        .toExternal("")
        .convertsFromInternal("NULL")
        .toExternal("NULL")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }
}
