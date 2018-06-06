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

import com.datastax.driver.dse.geometry.Point;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToPointCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private Point point = new Point(-1.1, -2.2);

  @Test
  void should_convert_from_valid_external() {
    StringToPointCodec codec = new StringToPointCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal("'POINT (-1.1 -2.2)'")
        .toInternal(point)
        .convertsFromExternal(" point (-1.1 -2.2) ")
        .toInternal(point)
        .convertsFromExternal("{\"type\":\"Point\",\"coordinates\":[-1.1,-2.2]}")
        .toInternal(point)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToPointCodec codec = new StringToPointCodec(nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal("POINT (-1.1 -2.2)");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToPointCodec codec = new StringToPointCodec(nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid point literal");
  }
}
