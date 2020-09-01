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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToIntegerCodecTest {

  private StringToIntegerCodec codec1;
  private StringToIntegerCodec codec2;

  @BeforeEach
  void setUpCodec1() {
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(new TextConversionContext());
    codec1 =
        (StringToIntegerCodec)
            codecFactory.<String, Integer>createConvertingCodec(
                DataTypes.INT, GenericType.STRING, true);
  }

  @BeforeEach
  void setUpCodec2() {
    ConversionContext context =
        new TextConversionContext().setNullStrings("NULL", "NADA").setFormatNumbers(true);
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec2 =
        (StringToIntegerCodec)
            codecFactory.<String, Integer>createConvertingCodec(
                DataTypes.INT, GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("0")
        .toInternal(0)
        .convertsFromExternal("2147483647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2147483648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0)
        .convertsFromExternal("TRUE")
        .toInternal(1)
        .convertsFromExternal("FALSE")
        .toInternal(0)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_external_with_custom_settings() {
    assertThat(codec2)
        .convertsFromExternal("0")
        .toInternal(0)
        .convertsFromExternal("2147483647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2147483648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0)
        .convertsFromExternal("TRUE")
        .toInternal(1)
        .convertsFromExternal("FALSE")
        .toInternal(0)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("NADA")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(0)
        .toExternal("0")
        .convertsFromInternal(Integer.MAX_VALUE)
        .toExternal("2147483647")
        .convertsFromInternal(Integer.MIN_VALUE)
        .toExternal("-2147483648")
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_convert_from_valid_internal_with_custom_settings() {
    assertThat(codec2)
        .convertsFromInternal(0)
        .toExternal("0")
        .convertsFromInternal(Integer.MAX_VALUE)
        .toExternal("2,147,483,647")
        .convertsFromInternal(Integer.MIN_VALUE)
        .toExternal("-2,147,483,648")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1)
        .cannotConvertFromExternal("NULL")
        .cannotConvertFromExternal("not a valid integer")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("2147483648")
        .cannotConvertFromExternal("-2147483649")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
    assertThat(codec2)
        .cannotConvertFromExternal("not a valid integer")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("2147483648")
        .cannotConvertFromExternal("-2147483649")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
