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
package com.datastax.oss.dsbulk.codecs.jdk.collection;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListToUDTCodecTest {

  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.DOUBLE)
          .build();

  private final UdtValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");

  private final UdtValue udt1Value = udt1.newValue().setInt("f1a", 42).setDouble("f1b", 0.12d);

  private ListToUDTCodec<String> udtCodec1 =
      (ListToUDTCodec<String>)
          new ConvertingCodecFactory()
              .<List<String>, UdtValue>createConvertingCodec(
                  udt1, GenericType.listOf(GenericType.STRING), true);

  private List<String> stringList = Arrays.asList("42", "0.12");

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(stringList)
        .toInternal(udt1Value)
        .convertsFromExternal(Arrays.asList("", null))
        .toInternal(udt1Empty)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(stringList)
        .convertsFromInternal(udt1.newValue())
        .toExternal(Arrays.asList("", ""))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(udtCodec1).cannotConvertFromExternal(Collections.singletonList(""));
  }
}
