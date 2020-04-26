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

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockTupleType;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListToTupleCodecTest {

  private TupleType tupleType =
      mockTupleType(V4, CodecRegistry.DEFAULT, DataTypes.TIMESTAMP, DataTypes.TEXT);

  private ListToTupleCodec<String> codec =
      (ListToTupleCodec<String>)
          new ConvertingCodecFactory()
              .<List<String>, TupleValue>createConvertingCodec(
                  tupleType, GenericType.listOf(GenericType.STRING), true);

  @Test
  void should_convert_when_valid_input() {
    TupleValue internal = tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00");
    List<String> external = Arrays.asList("2016-07-24T20:34:12.999Z", "+01:00");
    assertThat(codec)
        .convertsFromExternal(external)
        .toInternal(internal)
        .convertsFromInternal(internal)
        .toExternal(external)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {
    assertThat(codec)
        .cannotConvertFromExternal(Collections.singletonList("2016-07-24T20:34:12.999Z"));
  }
}
