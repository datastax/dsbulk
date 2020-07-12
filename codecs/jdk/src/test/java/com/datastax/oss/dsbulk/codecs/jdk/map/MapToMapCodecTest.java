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
package com.datastax.oss.dsbulk.codecs.jdk.map;

import static com.datastax.oss.driver.api.core.type.DataTypes.SMALLINT;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapToMapCodecTest {

  private final MapToMapCodec<Integer, Integer, Short, Short> codec =
      (MapToMapCodec<Integer, Integer, Short, Short>)
          new ConvertingCodecFactory()
              .<Map<Integer, Integer>, Map<Short, Short>>createConvertingCodec(
                  DataTypes.mapOf(SMALLINT, SMALLINT),
                  GenericType.mapOf(Integer.class, Integer.class),
                  true);

  private final Map<Integer, Integer> external =
      ImmutableMap.<Integer, Integer>builder().put(1, 99).put(2, 98).build();

  @Test
  void should_convert_when_valid_input() {
    // Convert Map<Integer, Integer> to Map<Short, Short>
    Map<Short, Short> shortInternal =
        ImmutableMap.<Short, Short>builder()
            .put((short) 1, (short) 99)
            .put((short) 2, (short) 98)
            .build();
    assertThat(codec)
        .convertsFromExternal(external)
        .toInternal(shortInternal)
        .convertsFromInternal(shortInternal)
        .toExternal(external)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
