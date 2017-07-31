/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.Assertions.assertThat;

import java.net.InetAddress;
import org.junit.Test;

public class StringToInetAddressCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(StringToInetAddressCodec.INSTANCE)
        .convertsFrom("1.2.3.4")
        .to(InetAddress.getByName("1.2.3.4"))
        .convertsFrom("127.0.0.1")
        .to(InetAddress.getByName("127.0.0.1"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(StringToInetAddressCodec.INSTANCE)
        .convertsTo(InetAddress.getByName("1.2.3.4"))
        .from("1.2.3.4")
        .convertsTo(InetAddress.getByName("127.0.0.1"))
        .from("127.0.0.1");
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(StringToInetAddressCodec.INSTANCE).cannotConvertFrom("not a valid inet address");
  }
}
