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

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class JsonNodeToInetAddressCodecTest {

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("1.2.3.4"))
        .to(InetAddress.getByName("1.2.3.4"))
        .convertsFrom(JSON_NODE_FACTORY.textNode("127.0.0.1"))
        .to(InetAddress.getByName("127.0.0.1"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .convertsTo(InetAddress.getByName("1.2.3.4"))
        .from(JSON_NODE_FACTORY.textNode("1.2.3.4"))
        .convertsTo(InetAddress.getByName("127.0.0.1"))
        .from(JSON_NODE_FACTORY.textNode("127.0.0.1"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid inet address"));
  }
}
