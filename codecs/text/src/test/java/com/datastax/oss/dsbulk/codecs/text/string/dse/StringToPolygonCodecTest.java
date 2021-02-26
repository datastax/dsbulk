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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.format.geo.JsonGeoFormat;
import com.datastax.oss.dsbulk.codecs.api.format.geo.WellKnownBinaryGeoFormat;
import com.datastax.oss.dsbulk.codecs.api.format.geo.WellKnownTextGeoFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToPolygonCodecTest {

  private final List<String> nullStrings = Lists.newArrayList("NULL");
  private final Polygon polygon =
      new DefaultPolygon(
          new DefaultPoint(30, 10),
          new DefaultPoint(10, 20),
          new DefaultPoint(20, 40),
          new DefaultPoint(40, 40));

  @Test
  void should_convert_from_valid_external() {
    StringToPolygonCodec codec =
        new StringToPolygonCodec(WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromExternal("'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'")
        .toInternal(polygon)
        .convertsFromExternal(" polygon ((30 10, 40 40, 20 40, 10 20, 30 10)) ")
        .toInternal(polygon)
        .convertsFromExternal(
            "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}")
        .toInternal(polygon)
        .convertsFromExternal(
            "AQMAAAABAAAABQAAAAAAAAAAAD5AAAAAAAAAJEAAAAAAAABEQAAAAAAAAERAAAAAAAAANEAAAAAAAABEQAAAAAAAACRAAAAAAAAANEAAAAAAAAA+QAAAAAAAACRA")
        .toInternal(polygon)
        .convertsFromExternal(
            "0x010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444"
                + "000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440")
        .toInternal(polygon)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToPolygonCodec codec =
        new StringToPolygonCodec(WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromInternal(polygon)
        .toExternal("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
    codec = new StringToPolygonCodec(JsonGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromInternal(polygon)
        .toExternal(
            "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}");
    codec = new StringToPolygonCodec(WellKnownBinaryGeoFormat.BASE64_INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromInternal(polygon)
        .toExternal(
            "AQMAAAABAAAABQAAAAAAAAAAAD5AAAAAAAAAJEAAAAAAAABEQAAAAAAAAERAAAAAAAAANEAAAAAAAABEQAAAAAAAACRAAAAAAAAANEAAAAAAAAA+QAAAAAAAACRA");
    codec = new StringToPolygonCodec(WellKnownBinaryGeoFormat.HEX_INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromInternal(polygon)
        .toExternal(
            "0x010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444"
                + "000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToPolygonCodec codec =
        new StringToPolygonCodec(WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid polygon literal");
  }
}
