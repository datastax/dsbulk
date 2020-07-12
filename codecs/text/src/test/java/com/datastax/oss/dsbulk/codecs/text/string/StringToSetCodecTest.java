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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToSetCodecTest {

  private StringToSetCodec<Double> codec1;
  private StringToSetCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec1 =
        (StringToSetCodec<Double>)
            codecFactory.<String, Set<Double>>createConvertingCodec(
                DataTypes.setOf(DataTypes.DOUBLE), GenericType.STRING, true);
    codec2 =
        (StringToSetCodec<String>)
            codecFactory.<String, Set<String>>createConvertingCodec(
                DataTypes.setOf(DataTypes.TEXT), GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("[1,2,3]")
        .toInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal("1,2,3")
        .toInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(" [  1 , 2 , 3 ] ")
        .toInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal("[1234.56,78900]")
        .toInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal("[\"1,234.56\",\"78,900\"]")
        .toInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal("[,]")
        .toInternal(Sets.newLinkedHashSet(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(Sets.newLinkedHashSet())
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal("[\"foo\",\"bar\"]")
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal("\"foo\",\"bar\"")
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal("['foo','bar']")
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(" [ \"foo\" , \"bar\" ] ")
        .toInternal(Sets.newLinkedHashSet("foo", "bar"))
        .convertsFromExternal("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]")
        .toInternal(Sets.newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFromExternal("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]")
        .toInternal(Sets.newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal("[,]")
        .toInternal(Sets.newLinkedHashSet(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(Sets.newLinkedHashSet(null, null))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal("[\"\",\"\"]")
        .toInternal(Sets.newLinkedHashSet(""))
        .convertsFromExternal("['','']")
        .toInternal(Sets.newLinkedHashSet(""))
        .convertsFromExternal("[\"NULL\",\"NULL\"]")
        .toInternal(Sets.newLinkedHashSet("NULL"))
        .convertsFromExternal("['NULL','NULL']")
        .toInternal(Sets.newLinkedHashSet("NULL"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(Sets.newLinkedHashSet())
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(Sets.newLinkedHashSet(1d, 2d, 3d))
        .toExternal("[1.0,2.0,3.0]")
        .convertsFromInternal(Sets.newLinkedHashSet(1234.56d, 78900d))
        .toExternal("[1234.56,78900.0]")
        .convertsFromInternal(Sets.newLinkedHashSet(1d, null))
        .toExternal("[1.0,null]")
        .convertsFromInternal(Sets.newLinkedHashSet(null, 0d))
        .toExternal("[null,0.0]")
        .convertsFromInternal(Sets.newLinkedHashSet((Double) null))
        .toExternal("[null]")
        .convertsFromInternal(Sets.newLinkedHashSet())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec2)
        .convertsFromInternal(Sets.newLinkedHashSet("foo", "bar"))
        .toExternal("[\"foo\",\"bar\"]")
        .convertsFromInternal(Sets.newLinkedHashSet("\"foo\"", "\"bar\""))
        .toExternal("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]")
        .convertsFromInternal(Sets.newLinkedHashSet("\\foo\\", "\\bar\\"))
        .toExternal("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]")
        .convertsFromInternal(Sets.newLinkedHashSet(",foo,", ",bar,"))
        .toExternal("[\",foo,\",\",bar,\"]")
        .convertsFromInternal(Sets.newLinkedHashSet(""))
        .toExternal("[\"\"]")
        .convertsFromInternal(Sets.newLinkedHashSet((String) null))
        .toExternal("[null]")
        .convertsFromInternal(Sets.newLinkedHashSet())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1)
        .cannotConvertFromExternal("[1,\"not a valid double\"]")
        .cannotConvertFromExternal("[ \"not a valid array\" : 42 ")
        .cannotConvertFromExternal("[42");
  }
}
