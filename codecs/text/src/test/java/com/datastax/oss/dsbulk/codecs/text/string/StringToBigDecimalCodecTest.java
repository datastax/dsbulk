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
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToBigDecimalCodecTest {

  StringToBigDecimalCodec codec;

  @BeforeEach
  void setUp() {
    ConversionContext context =
        new TextConversionContext().setNullStrings("NULL").setFormatNumbers(true);
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (StringToBigDecimalCodec)
            codecFactory.<String, BigDecimal>createConvertingCodec(
                DataTypes.DECIMAL, GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(ZERO)
        .convertsFromExternal("-1234.56")
        .toInternal(new BigDecimal("-1234.56"))
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(new BigDecimal("0"))
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(new BigDecimal("946684800000"))
        .convertsFromExternal("true")
        .toInternal(new BigDecimal("1"))
        .convertsFromExternal("false")
        .toInternal(new BigDecimal("0"))
        .convertsFromExternal("TRUE")
        .toInternal(ONE)
        .convertsFromExternal("FALSE")
        .toInternal(ZERO)
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
        .convertsFromInternal(ZERO)
        .toExternal("0")
        .convertsFromInternal(new BigDecimal("1234.56"))
        .toExternal("1,234.56")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid decimal");
  }
}
