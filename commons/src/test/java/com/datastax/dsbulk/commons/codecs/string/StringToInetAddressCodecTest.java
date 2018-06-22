/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class StringToInetAddressCodecTest {

  private StringToInetAddressCodec codec = new StringToInetAddressCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
        .convertsFromExternal("1.2.3.4")
        .toInternal(InetAddress.getByName("1.2.3.4"))
        .convertsFromExternal("127.0.0.1")
        .toInternal(InetAddress.getByName("127.0.0.1"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
        .convertsFromInternal(InetAddress.getByName("1.2.3.4"))
        .toExternal("1.2.3.4")
        .convertsFromInternal(InetAddress.getByName("127.0.0.1"))
        .toExternal("127.0.0.1")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid inet address");
  }
}
