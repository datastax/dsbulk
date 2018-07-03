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
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToBigDecimalCodecTest {

  StringToBigDecimalCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToBigDecimalCodec)
            newCodecRegistry("nullStrings = [NULL], formatNumbers = true")
                .codecFor(DataTypes.DECIMAL, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(ZERO)
        .convertsFromExternal("-1234.56")
        .toInternal(new BigDecimal("-1234.56"))
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(new BigDecimal("0"))
        .convertsFromExternal("2000-01-01T00:00:00Z")
        .toInternal(new BigDecimal("946684800000"))
        .convertsFromExternal("true")
        .toInternal(new BigDecimal("1"))
        .convertsFromExternal("false")
        .toInternal(new BigDecimal("0"))
        .convertsFromExternal("TRUE")
        .toInternal(ONE)
        .convertsFromExternal("FALSE")
        .toInternal(ZERO)
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
        .convertsFromInternal(ZERO)
        .toExternal("0")
        .convertsFromInternal(new BigDecimal("1234.56"))
        .toExternal("1,234.56")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid decimal");
  }
}
