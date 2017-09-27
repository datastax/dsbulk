/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.Test;

public class JsonNodeToBlobCodecTest {

  private static final byte[] DATA = {1, 2, 3, 4, 5, 6};
  private static final byte[] EMPTY = {};

  private static final ByteBuffer DATA_BB = ByteBuffer.wrap(DATA);
  private static final ByteBuffer EMPTY_BB = ByteBuffer.wrap(EMPTY);

  private static final String DATA_64 = Base64.getEncoder().encodeToString(DATA);
  private static final String DATA_HEX = Bytes.toHexString(DATA);

  private static final String EMPTY_HEX = "0x";

  JsonNodeToBlobCodec codec = JsonNodeToBlobCodec.INSTANCE;

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.binaryNode(DATA))
        .to(DATA_BB)
        .convertsFrom(JsonNodeFactory.instance.binaryNode(EMPTY))
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(DATA_64))
        .to(DATA_BB)
        .convertsFrom(JsonNodeFactory.instance.textNode(DATA_HEX))
        .to(DATA_BB)
        .convertsFrom(JsonNodeFactory.instance.textNode(EMPTY_HEX))
        .to(EMPTY_BB)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.nullNode())
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(DATA_BB)
        .from(JsonNodeFactory.instance.binaryNode(DATA))
        .convertsTo(EMPTY_BB)
        .from(JsonNodeFactory.instance.binaryNode(EMPTY))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid binary"));
  }
}
