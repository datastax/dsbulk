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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.api.CommonConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapToUDTCodecTest {

  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.DOUBLE)
          .build();

  private final UdtValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");

  private final UdtValue udt1Value = udt1.newValue().setInt("f1a", 42).setDouble("f1b", 0.12d);

  private final MapToUDTCodec<String, String> udtCodec1 =
      (MapToUDTCodec<String, String>)
          new ConvertingCodecFactory(new CommonConversionContext())
              .<Map<String, String>, UdtValue>createConvertingCodec(
                  udt1, GenericType.mapOf(GenericType.STRING, GenericType.STRING), true);

  private final ImmutableMap<String, String> stringMap =
      ImmutableMap.<String, String>builder().put("f1a", "42").put("f1b", "0.12").build();

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(stringMap)
        .toInternal(udt1Value)
        .convertsFromExternal(NullAllowingImmutableMap.of("f1a", "", "f1b", null))
        .toInternal(udt1Empty)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(stringMap)
        .convertsFromInternal(udt1.newValue())
        .toExternal(NullAllowingImmutableMap.of("f1a", null, "f1b", null))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(udtCodec1)
        .cannotConvertFromExternal(ImmutableMap.<String, String>builder().put("f1a", "").build())
        .cannotConvertFromExternal(
            ImmutableMap.<String, String>builder().put("f1a", "").put("f1c", "").build());
  }
}
