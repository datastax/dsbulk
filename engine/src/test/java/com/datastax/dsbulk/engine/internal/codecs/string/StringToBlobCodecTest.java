/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.Test;

public class StringToBlobCodecTest {

  private static final byte[] DATA = {1, 2, 3, 4, 5, 6};
  private static final byte[] EMPTY = {};

  private static final ByteBuffer DATA_BB = ByteBuffer.wrap(DATA);
  private static final ByteBuffer EMPTY_BB = ByteBuffer.wrap(EMPTY);

  private static final String DATA_64 = Base64.getEncoder().encodeToString(DATA);
  private static final String DATA_HEX = Bytes.toHexString(DATA);

  private static final String EMPTY_HEX = "0x";

  StringToBlobCodec codec = StringToBlobCodec.INSTANCE;

  @Test
  public void should_convert_from_valid_input() throws Exception {
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
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(DATA_BB)
        .from(DATA_64)
        .convertsTo(EMPTY_BB)
        .from("")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid binary");
  }
}
