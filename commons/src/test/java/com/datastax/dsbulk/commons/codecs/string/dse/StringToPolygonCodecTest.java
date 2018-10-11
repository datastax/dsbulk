/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string.dse;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToPolygonCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private Polygon polygon =
      new DefaultPolygon(
          new DefaultPoint(30, 10),
          new DefaultPoint(10, 20),
          new DefaultPoint(20, 40),
          new DefaultPoint(40, 40));

  @Test
  void should_convert_from_valid_external() {
    StringToPolygonCodec codec = new StringToPolygonCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal("'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'")
        .toInternal(polygon)
        .convertsFromExternal(" polygon ((30 10, 40 40, 20 40, 10 20, 30 10)) ")
        .toInternal(polygon)
        .convertsFromExternal(
            "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}")
        .toInternal(polygon)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToPolygonCodec codec = new StringToPolygonCodec(nullStrings);
    assertThat(codec)
        .convertsFromInternal(polygon)
        .toExternal("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToPolygonCodec codec = new StringToPolygonCodec(nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid polygon literal");
  }
}
