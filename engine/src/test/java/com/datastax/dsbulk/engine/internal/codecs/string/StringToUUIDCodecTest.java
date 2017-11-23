/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import java.util.UUID;
import org.junit.Test;

public class StringToUUIDCodecTest {

  StringToUUIDCodec codec = new StringToUUIDCodec(TypeCodec.uuid());

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("a15341ec-ebef-4eab-b91d-ff16bf801a79")
        .to(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .from("a15341ec-ebef-4eab-b91d-ff16bf801a79");
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid UUID");
  }
}
