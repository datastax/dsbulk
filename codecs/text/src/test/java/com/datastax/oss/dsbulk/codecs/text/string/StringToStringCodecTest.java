/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToStringCodecTest {

  private List<String> nullStrings = Lists.newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    StringToStringCodec codec = new StringToStringCodec(TypeCodecs.TEXT, nullStrings);
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
    StringToStringCodec codec = new StringToStringCodec(TypeCodecs.TEXT, nullStrings);
    assertThat(codec)
        .convertsFromInternal("foo")
        .toExternal("foo")
        .convertsFromInternal(null)
        .toExternal("NULL")
        .convertsFromInternal("")
        .toExternal("");
  }
}
