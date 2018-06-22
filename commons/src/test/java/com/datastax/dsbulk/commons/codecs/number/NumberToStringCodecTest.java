/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Test;

class NumberToStringCodecTest {

  private NumberToStringCodec<Long> codec =
      (NumberToStringCodec<Long>)
          new ExtendedCodecRegistryBuilder()
              .withNullStrings("NULL")
              .withFormatNumbers(true)
              .build()
              .<Long, String>convertingCodecFor(DataTypes.TEXT, GenericType.LONG);

  @Test
  void should_convert_when_valid_input() {
    assertThat(codec)
        .convertsFromExternal(123456L)
        .toInternal("123,456")
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null)
        .convertsFromInternal("")
        .toExternal(null);
  }
}
