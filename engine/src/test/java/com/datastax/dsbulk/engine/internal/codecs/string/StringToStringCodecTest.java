/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.core.TypeCodec;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToStringCodecTest {

  private List<String> nullStrings = newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    StringToStringCodec codec = new StringToStringCodec(TypeCodec.varchar(), nullStrings);
    assertThat(codec)
        .convertsFromExternal("foo")
        .toInternal("foo")
        .convertsFromExternal("")
        .toInternal("")
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToStringCodec codec = new StringToStringCodec(TypeCodec.varchar(), nullStrings);
    assertThat(codec)
        .convertsFromInternal("foo")
        .toExternal("foo")
        .convertsFromInternal(null)
        .toExternal("NULL")
        .convertsFromInternal("")
        .toExternal("");
  }
}
