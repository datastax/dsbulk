/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Test;

class BooleanToStringCodecTest {
  private BooleanToStringCodec codec =
      (BooleanToStringCodec)
          newCodecRegistry("")
              .<Boolean, String>convertingCodecFor(DataTypes.TEXT, GenericType.BOOLEAN);

  @Test
  void should_convert_when_valid_input() {
    assertThat(codec)
        .convertsFromExternal(true)
        .toInternal("true")
        .convertsFromExternal(false)
        .toInternal("false")
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null)
        .convertsFromInternal("")
        .toExternal(null);
  }
}
