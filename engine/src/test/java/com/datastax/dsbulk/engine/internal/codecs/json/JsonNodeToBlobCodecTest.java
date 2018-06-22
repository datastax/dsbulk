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

import com.datastax.oss.protocol.internal.util.Bytes;
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
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(data))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(empty))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(data64))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(dataHex))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0x"))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(dataBb)
        .toExternal(JSON_NODE_FACTORY.binaryNode(data))
        .convertsFromInternal(emptyBb)
        .toExternal(JSON_NODE_FACTORY.binaryNode(empty))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid binary"));
  }
}
