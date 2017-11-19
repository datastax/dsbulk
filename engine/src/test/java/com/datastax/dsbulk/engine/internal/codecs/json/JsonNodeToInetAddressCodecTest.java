/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class JsonNodeToInetAddressCodecTest {

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .convertsFrom(JsonNodeFactory.instance.textNode("1.2.3.4"))
        .to(InetAddress.getByName("1.2.3.4"))
        .convertsFrom(JsonNodeFactory.instance.textNode("127.0.0.1"))
        .to(InetAddress.getByName("127.0.0.1"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .convertsTo(InetAddress.getByName("1.2.3.4"))
        .from(JsonNodeFactory.instance.textNode("1.2.3.4"))
        .convertsTo(InetAddress.getByName("127.0.0.1"))
        .from(JsonNodeFactory.instance.textNode("127.0.0.1"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(JsonNodeToInetAddressCodec.INSTANCE)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid inet address"));
  }
}
