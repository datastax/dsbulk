/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DataType.tinyint;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToByteCodecTest {

  private StringToByteCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToByteCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(tinyint(), TypeToken.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal((byte) 0)
        .convertsFromExternal("127")
        .toInternal((byte) 127)
        .convertsFromExternal("-128")
        .toInternal((byte) -128)
        .convertsFromExternal("0")
        .toInternal((byte) 0)
        .convertsFromExternal("127")
        .toInternal((byte) 127)
        .convertsFromExternal("-128")
        .toInternal((byte) -128)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal((byte) 0)
        .convertsFromExternal("TRUE")
        .toInternal((byte) 1)
        .convertsFromExternal("FALSE")
        .toInternal((byte) 0)
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
        .convertsFromInternal((byte) 0)
        .toExternal("0")
        .convertsFromInternal((byte) 127)
        .toExternal("127")
        .convertsFromInternal((byte) -128)
        .toExternal("-128")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid byte")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("128")
        .cannotConvertFromExternal("-129")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
