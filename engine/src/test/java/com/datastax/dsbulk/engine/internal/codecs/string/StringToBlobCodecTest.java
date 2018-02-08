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

  private final byte[] data = {1, 2, 3, 4, 5, 6};
  private final byte[] empty = {};

  private final ByteBuffer dataBb = ByteBuffer.wrap(data);
  private final ByteBuffer emptyBb = ByteBuffer.wrap(empty);

  private final String data64 = Base64.getEncoder().encodeToString(data);
  private final String dataHex = Bytes.toHexString(data);

  private final StringToBlobCodec codec = StringToBlobCodec.INSTANCE;

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(data64)
        .to(dataBb)
        .convertsFrom(dataHex)
        .to(dataBb)
        .convertsFrom("0x")
        .to(emptyBb)
        .convertsFrom("")
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(dataBb)
        .from(data64)
        .convertsTo(emptyBb)
        .from("")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid binary");
  }
}
