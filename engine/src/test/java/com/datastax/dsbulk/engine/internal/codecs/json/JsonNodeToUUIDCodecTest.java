/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.UUID;
import org.junit.Test;

public class JsonNodeToUUIDCodecTest {

  JsonNodeToUUIDCodec codec = new JsonNodeToUUIDCodec(TypeCodec.uuid());

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .to(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .from(JsonNodeFactory.instance.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid UUID"));
  }
}
