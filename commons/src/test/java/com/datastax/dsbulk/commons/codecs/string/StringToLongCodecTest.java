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

class StringToLongCodecTest {

  private StringToLongCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToLongCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withFormatNumbers(true)
                .build()
                .codecFor(DataTypes.BIGINT, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0L)
        .convertsFromExternal("9223372036854775807")
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal("-9223372036854775808")
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal("9,223,372,036,854,775,807")
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal("-9,223,372,036,854,775,808")
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0L)
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(946684800000L)
        .convertsFromExternal("TRUE")
        .toInternal(1L)
        .convertsFromExternal("FALSE")
        .toInternal(0L)
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
        .convertsFromInternal(0L)
        .toExternal("0")
        .convertsFromInternal(Long.MAX_VALUE)
        .toExternal("9,223,372,036,854,775,807")
        .convertsFromInternal(Long.MIN_VALUE)
        .toExternal("-9,223,372,036,854,775,808")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid long")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("9223372036854775808")
        .cannotConvertFromExternal("-9223372036854775809");
  }
}
