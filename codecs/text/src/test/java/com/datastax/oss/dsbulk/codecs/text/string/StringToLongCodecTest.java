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
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToLongCodecTest {

  private StringToLongCodec codec;

  @BeforeEach
  void setUp() {
    ConversionContext context =
        new TextConversionContext().setNullStrings("NULL").setFormatNumbers(true);
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (StringToLongCodec)
            codecFactory.<String, Long>createConvertingCodec(
                DataTypes.BIGINT, GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0L)
        .convertsFromExternal("9223372036854775807")
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal("-9223372036854775808")
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal("9,223,372,036,854,775,807")
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal("-9,223,372,036,854,775,808")
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0L)
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(946684800000L)
        .convertsFromExternal("TRUE")
        .toInternal(1L)
        .convertsFromExternal("FALSE")
        .toInternal(0L)
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
        .convertsFromInternal(0L)
        .toExternal("0")
        .convertsFromInternal(Long.MAX_VALUE)
        .toExternal("9,223,372,036,854,775,807")
        .convertsFromInternal(Long.MIN_VALUE)
        .toExternal("-9,223,372,036,854,775,808")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid long")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("9223372036854775808")
        .cannotConvertFromExternal("-9223372036854775809");
  }
}
