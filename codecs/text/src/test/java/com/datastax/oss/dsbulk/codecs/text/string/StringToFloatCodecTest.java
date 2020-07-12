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
import static java.math.RoundingMode.HALF_EVEN;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToFloatCodecTest {

  private StringToFloatCodec codec;

  @BeforeEach
  void setUp() {
    ConversionContext context =
        new TextConversionContext()
            .setNullStrings("NULL")
            .setFormatNumbers(true)
            .setRoundingMode(HALF_EVEN);
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (StringToFloatCodec)
            codecFactory.<String, Float>createConvertingCodec(
                DataTypes.FLOAT, GenericType.STRING, true);
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0f)
        .convertsFromExternal("1234.56")
        .toInternal(1234.56f)
        .convertsFromExternal("1,234.56")
        .toInternal(1234.56f)
        .convertsFromExternal("3.4028235E38")
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal("1.4E-45")
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal("0.0000000000000000000000000000000000000000000014")
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0f)
        .convertsFromExternal("TRUE")
        .toInternal(1f)
        .convertsFromExternal("FALSE")
        .toInternal(0f)
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
        .convertsFromInternal(0f)
        .toExternal("0")
        .convertsFromInternal(1234.56f)
        .toExternal("1,234.56")
        .convertsFromInternal(Float.MAX_VALUE)
        .toExternal("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .convertsFromInternal(0.001f)
        .toExternal("0") // decimals truncated
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid float");
  }
}
