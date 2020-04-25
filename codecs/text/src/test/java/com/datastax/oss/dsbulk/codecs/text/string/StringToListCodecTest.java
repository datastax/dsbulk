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
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToListCodecTest {

  private StringToListCodec<Double> codec1;
  private StringToListCodec<Instant> codec2;
  private StringToListCodec<String> codec3;

  private Instant i1 = Instant.parse("2016-07-24T20:34:12.999Z");
  private Instant i2 = Instant.parse("2018-05-25T18:34:12.999Z");

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec1 =
        (StringToListCodec<Double>)
            codecFactory.<String, List<Double>>createConvertingCodec(
                DataTypes.listOf(DataTypes.DOUBLE), GenericType.STRING, true);
    codec2 =
        (StringToListCodec<Instant>)
            codecFactory.<String, List<Instant>>createConvertingCodec(
                DataTypes.listOf(DataTypes.TIMESTAMP), GenericType.STRING, true);
    codec3 =
        (StringToListCodec<String>)
            codecFactory.<String, List<String>>createConvertingCodec(
                DataTypes.listOf(DataTypes.TEXT), GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("[1,2,3]")
        .toInternal(Lists.newArrayList(1d, 2d, 3d))
        .convertsFromExternal("1,2,3")
        .toInternal(Lists.newArrayList(1d, 2d, 3d))
        .convertsFromExternal(" [  1 , 2 , 3 ] ")
        .toInternal(Lists.newArrayList(1d, 2d, 3d))
        .convertsFromExternal("[1234.56,78900]")
        .toInternal(Lists.newArrayList(1234.56d, 78900d))
        .convertsFromExternal("[\"1,234.56\",\"78,900\"]")
        .toInternal(Lists.newArrayList(1234.56d, 78900d))
        .convertsFromExternal("[,]")
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(Lists.newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999Z\",\"2018-05-25 20:34:12.999+02:00\"]")
        .toInternal(Lists.newArrayList(i1, i2))
        .convertsFromExternal("\"2016-07-24T20:34:12.999Z\",\"2018-05-25 20:34:12.999+02:00\"")
        .toInternal(Lists.newArrayList(i1, i2))
        .convertsFromExternal("['2016-07-24T20:34:12.999Z','2018-05-25 20:34:12.999+02:00']")
        .toInternal(Lists.newArrayList(i1, i2))
        .convertsFromExternal(
            " [ \"2016-07-24T20:34:12.999Z\" , \"2018-05-25 20:34:12.999+02:00\" ] ")
        .toInternal(Lists.newArrayList(i1, i2))
        .convertsFromExternal("[,]")
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(Lists.newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec3)
        .convertsFromExternal("[\"foo\",\"bar\"]")
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal("['foo','bar']")
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal(" [ \"foo\" , \"bar\" ] ")
        .toInternal(Lists.newArrayList("foo", "bar"))
        .convertsFromExternal("[,]")
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(Lists.newArrayList(null, null))
        .convertsFromExternal("['','']")
        .toInternal(Lists.newArrayList("", ""))
        .convertsFromExternal("[\"\",\"\"]")
        .toInternal(Lists.newArrayList("", ""))
        .convertsFromExternal("[\"NULL\",\"NULL\"]")
        .toInternal(Lists.newArrayList("NULL", "NULL"))
        .convertsFromExternal("['NULL','NULL']")
        .toInternal(Lists.newArrayList("NULL", "NULL"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(Lists.newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(Lists.newArrayList(1d, 2d, 3d))
        .toExternal("[1.0,2.0,3.0]")
        .convertsFromInternal(Lists.newArrayList(1234.56d, 78900d))
        .toExternal("[1234.56,78900.0]")
        .convertsFromInternal(Lists.newArrayList(1d, null))
        .toExternal("[1.0,null]")
        .convertsFromInternal(Lists.newArrayList(null, 0d))
        .toExternal("[null,0.0]")
        .convertsFromInternal(Lists.newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(Lists.newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec2)
        .convertsFromInternal(Lists.newArrayList(i1, i2))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"2018-05-25T18:34:12.999Z\"]")
        .convertsFromInternal(Lists.newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(Lists.newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec3)
        .convertsFromInternal(Lists.newArrayList("foo", "bar"))
        .toExternal("[\"foo\",\"bar\"]")
        .convertsFromInternal(Lists.newArrayList("", ""))
        .toExternal("[\"\",\"\"]")
        .convertsFromInternal(Lists.newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(Lists.newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1).cannotConvertFromExternal("[1,\"not a valid double\"]");
  }
}
