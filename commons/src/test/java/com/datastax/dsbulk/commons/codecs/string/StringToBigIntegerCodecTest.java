/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.math.BigInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToBigIntegerCodecTest {

  private StringToBigIntegerCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToBigIntegerCodec)
            newCodecRegistry("nullStrings = [NULL], formatNumbers = true")
                .codecFor(DataTypes.VARINT, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(BigInteger.ZERO)
        .convertsFromExternal("-1,234")
        .toInternal(new BigInteger("-1234"))
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(new BigInteger("0"))
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(new BigInteger("946684800000"))
        .convertsFromExternal("TRUE")
        .toInternal(BigInteger.ONE)
        .convertsFromExternal("FALSE")
        .toInternal(BigInteger.ZERO)
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
        .convertsFromInternal(BigInteger.ZERO)
        .toExternal("0")
        .convertsFromInternal(new BigInteger("-1234"))
        .toExternal("-1,234")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("-1.234")
        .cannotConvertFromExternal("not a valid biginteger");
  }
}
