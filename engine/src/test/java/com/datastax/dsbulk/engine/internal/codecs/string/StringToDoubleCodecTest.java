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

class StringToDoubleCodecTest {

  private StringToDoubleCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToDoubleCodec)
            newCodecRegistry(
                    "nullStrings = [NULL], roundingStrategy = HALF_EVEN, formatNumbers = true")
                .codecFor(DataTypes.DOUBLE, GenericType.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0d)
        .convertsFromExternal("1234.56")
        .toInternal(1234.56d)
        .convertsFromExternal("1,234.56")
        .toInternal(1234.56d)
        .convertsFromExternal("1.7976931348623157E308")
        .toInternal(Double.MAX_VALUE)
        .convertsFromExternal("4.9E-324")
        .toInternal(Double.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0d)
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(946684800000d)
        .convertsFromExternal("TRUE")
        .toInternal(1d)
        .convertsFromExternal("FALSE")
        .toInternal(0d)
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
        .convertsFromInternal(0d)
        .toExternal("0")
        .convertsFromInternal(1234.56d)
        .toExternal("1,234.56")
        .convertsFromInternal(0.001)
        .toExternal("0")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid double");
  }
}
