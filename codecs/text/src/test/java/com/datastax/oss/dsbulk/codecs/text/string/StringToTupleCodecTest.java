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

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.dsbulk.tests.driver.DriverUtils;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToTupleCodecTest {

  private TupleType tupleType;

  private StringToTupleCodec codec1;
  private StringToTupleCodec codec2;
  private StringToTupleCodec codec3;

  @BeforeEach
  void setUp() {
    tupleType =
        DriverUtils.mockTupleType(
            DefaultProtocolVersion.V4, CodecRegistry.DEFAULT, DataTypes.TIMESTAMP, DataTypes.TEXT);
    ConversionContext context1 = new TextConversionContext().setNullStrings("NULL", "");
    ConversionContext context2 = new TextConversionContext().setAllowExtraFields(true);
    ConversionContext context3 = new TextConversionContext().setAllowMissingFields(true);
    codec1 =
        (StringToTupleCodec)
            new ConvertingCodecFactory(context1)
                .<String, TupleValue>createConvertingCodec(tupleType, GenericType.STRING, true);
    codec2 =
        (StringToTupleCodec)
            new ConvertingCodecFactory(context2)
                .<String, TupleValue>createConvertingCodec(tupleType, GenericType.STRING, true);
    codec3 =
        (StringToTupleCodec)
            new ConvertingCodecFactory(context3)
                .<String, TupleValue>createConvertingCodec(tupleType, GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\",\"+01:00\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("['2016-07-24T20:34:12.999','+01:00']")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[null,\"\"]")
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal("[null,\"NULL\"]")
        .toInternal(tupleType.newValue(null, "NULL"))
        .convertsFromExternal("[null,null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[,]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    // should allow extra elements
    assertThat(codec2)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\",\"+01:00\", 42]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[,\"\",\"\"]")
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal("[null,null,null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[,,]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    // should allow missing elements
    assertThat(codec3)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), null))
        .convertsFromExternal("[null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(
            tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .convertsFromInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), ""))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"\"]")
        .convertsFromInternal(tupleType.newValue(null, ""))
        .toExternal("[null,\"\"]")
        .convertsFromInternal(tupleType.newValue(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1).cannotConvertFromExternal("{\"not a valid tuple\":42}");
    // should not allow missing elements
    assertThat(codec2)
        .cannotConvertFromExternal("[\"2016-07-24T20:34:12.999Z\"]")
        .cannotConvertFromExternal("[]");
    // should not allow extra elements
    assertThat(codec3).cannotConvertFromExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\",42]");
  }
}
