/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToShortCodecTest {

  private StringToShortCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToShortCodec)
            newCodecRegistry("nullStrings = [NULL], formatNumbers = true")
                .codecFor(DataTypes.SMALLINT, GenericType.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal((short) 0)
        .convertsFromExternal("32767")
        .toInternal((short) 32767)
        .convertsFromExternal("-32768")
        .toInternal((short) -32768)
        .convertsFromExternal("32,767")
        .toInternal((short) 32767)
        .convertsFromExternal("-32,768")
        .toInternal((short) -32768)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal((short) 0)
        .convertsFromExternal("TRUE")
        .toInternal((short) 1)
        .convertsFromExternal("FALSE")
        .toInternal((short) 0)
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
        .convertsFromInternal((short) 0)
        .toExternal("0")
        .convertsFromInternal((short) 32767)
        .toExternal("32,767")
        .convertsFromInternal((short) -32768)
        .toExternal("-32,768")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid short")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("32768")
        .cannotConvertFromExternal("-32769")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
