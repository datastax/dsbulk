/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class StringToBlobCodecTest {

  private static final byte[] DATA = {1, 2, 3, 4, 5, 6};
  private static final byte[] EMPTY = {};

  private static final ByteBuffer DATA_BB = ByteBuffer.wrap(DATA);
  private static final ByteBuffer EMPTY_BB = ByteBuffer.wrap(EMPTY);

  private static final String DATA_64 = Base64.getEncoder().encodeToString(DATA);
  private static final String DATA_HEX = Bytes.toHexString(DATA);

  private static final String EMPTY_HEX = "0x";

  private final StringToBlobCodec codec = StringToBlobCodec.INSTANCE;

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(DATA_64)
        .to(DATA_BB)
        .convertsFrom(DATA_HEX)
        .to(DATA_BB)
        .convertsFrom(EMPTY_HEX)
        .to(EMPTY_BB)
        .convertsFrom("")
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(DATA_BB)
        .from(DATA_64)
        .convertsTo(EMPTY_BB)
        .from("")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid binary");
  }
}
