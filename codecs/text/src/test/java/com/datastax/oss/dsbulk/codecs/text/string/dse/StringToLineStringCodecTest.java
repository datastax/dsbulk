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
package com.datastax.oss.dsbulk.codecs.text.string.dse;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.tests.assertions.TestAssertions;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLineStringCodecTest {

  private List<String> nullStrings = Lists.newArrayList("NULL");
  private LineString lineString =
      new DefaultLineString(
          new DefaultPoint(30, 10), new DefaultPoint(10, 30), new DefaultPoint(40, 40));

  @Test
  void should_convert_from_valid_external() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    TestAssertions.assertThat(codec)
        .convertsFromExternal("'LINESTRING (30 10, 10 30, 40 40)'")
        .toInternal(lineString)
        .convertsFromExternal(" linestring (30 10, 10 30, 40 40) ")
        .toInternal(lineString)
        .convertsFromExternal(
            "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}")
        .toInternal(lineString)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    TestAssertions.assertThat(codec)
        .convertsFromInternal(lineString)
        .toExternal("LINESTRING (30 10, 10 30, 40 40)");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    TestAssertions.assertThat(codec).cannotConvertFromExternal("not a valid linestring literal");
  }
}
