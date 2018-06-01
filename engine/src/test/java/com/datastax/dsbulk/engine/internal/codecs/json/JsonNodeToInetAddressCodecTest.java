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

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class JsonNodeToInetAddressCodecTest {

  private final JsonNodeToInetAddressCodec codec =
      new JsonNodeToInetAddressCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1.2.3.4"))
        .toInternal(InetAddress.getByName("1.2.3.4"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("127.0.0.1"))
        .toInternal(InetAddress.getByName("127.0.0.1"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
        .convertsFromInternal(InetAddress.getByName("1.2.3.4"))
        .toExternal(JSON_NODE_FACTORY.textNode("1.2.3.4"))
        .convertsFromInternal(InetAddress.getByName("127.0.0.1"))
        .toExternal(JSON_NODE_FACTORY.textNode("127.0.0.1"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid inet address"));
  }
}
