/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.core.utils.Bytes;
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

  private final JsonNodeToBlobCodec codec = new JsonNodeToBlobCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.binaryNode(data))
        .to(dataBb)
        .convertsFrom(JSON_NODE_FACTORY.binaryNode(empty))
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(data64))
        .to(dataBb)
        .convertsFrom(JSON_NODE_FACTORY.textNode(dataHex))
        .to(dataBb)
        .convertsFrom(JSON_NODE_FACTORY.textNode("0x"))
        .to(emptyBb)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.nullNode())
        .to(null)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode("NULL"))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(dataBb)
        .from(JSON_NODE_FACTORY.binaryNode(data))
        .convertsTo(emptyBb)
        .from(JSON_NODE_FACTORY.binaryNode(empty))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid binary"));
  }
}
