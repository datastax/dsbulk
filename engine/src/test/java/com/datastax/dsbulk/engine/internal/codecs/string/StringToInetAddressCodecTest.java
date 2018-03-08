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
import static com.google.common.collect.Lists.newArrayList;

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class StringToInetAddressCodecTest {

  private StringToInetAddressCodec codec = new StringToInetAddressCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("1.2.3.4")
        .to(InetAddress.getByName("1.2.3.4"))
        .convertsFrom("127.0.0.1")
        .to(InetAddress.getByName("127.0.0.1"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("NULL")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(InetAddress.getByName("1.2.3.4"))
        .from("1.2.3.4")
        .convertsTo(InetAddress.getByName("127.0.0.1"))
        .from("127.0.0.1")
        .convertsTo(null)
        .from("NULL");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid inet address");
  }
}
