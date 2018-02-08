/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class JsonNodeToBlobCodecTest {

  private final byte[] data = {1, 2, 3, 4, 5, 6};
  private final byte[] empty = {};

  private final ByteBuffer dataBb = ByteBuffer.wrap(data);
  private final ByteBuffer emptyBb = ByteBuffer.wrap(empty);

  private final String data64 = Base64.getEncoder().encodeToString(data);
  private final String dataHex = Bytes.toHexString(data);

  private final JsonNodeToBlobCodec codec = JsonNodeToBlobCodec.INSTANCE;

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.binaryNode(data))
        .to(dataBb)
        .convertsFrom(JsonNodeFactory.instance.binaryNode(empty))
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(data64))
        .to(dataBb)
        .convertsFrom(JsonNodeFactory.instance.textNode(dataHex))
        .to(dataBb)
        .convertsFrom(JsonNodeFactory.instance.textNode("0x"))
        .to(emptyBb)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.nullNode())
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(dataBb)
        .from(JsonNodeFactory.instance.binaryNode(data))
        .convertsTo(emptyBb)
        .from(JsonNodeFactory.instance.binaryNode(empty))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid binary"));
  }
}
