/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string.dse;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.dse.geometry.LineString;
import com.datastax.driver.dse.geometry.Point;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLineStringCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private LineString lineString =
      new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40));

  @Test
  void should_convert_from_valid_external() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal("'LINESTRING (30 10, 10 30, 40 40)'")
        .toInternal(lineString)
        .convertsFromExternal(" linestring (30 10, 10 30, 40 40) ")
        .toInternal(lineString)
        .convertsFromExternal(
            "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}")
        .toInternal(lineString)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    assertThat(codec)
        .convertsFromInternal(lineString)
        .toExternal("LINESTRING (30 10, 10 30, 40 40)");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLineStringCodec codec = new StringToLineStringCodec(nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid linestring literal");
  }
}
