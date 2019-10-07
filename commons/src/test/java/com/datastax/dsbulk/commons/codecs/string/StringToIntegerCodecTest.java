/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToIntegerCodecTest {

  private StringToIntegerCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToIntegerCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withFormatNumbers(true)
                .build()
                .codecFor(DataTypes.INT, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0)
        .convertsFromExternal("2147483647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2147483648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0)
        .convertsFromExternal("TRUE")
        .toInternal(1)
        .convertsFromExternal("FALSE")
        .toInternal(0)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0)
        .toExternal("0")
        .convertsFromInternal(Integer.MAX_VALUE)
        .toExternal("2,147,483,647")
        .convertsFromInternal(Integer.MIN_VALUE)
        .toExternal("-2,147,483,648")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid integer")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("2147483648")
        .cannotConvertFromExternal("-2147483649")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
