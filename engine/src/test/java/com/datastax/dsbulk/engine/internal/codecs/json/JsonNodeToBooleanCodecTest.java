/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class JsonNodeToBooleanCodecTest {

  Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();
  JsonNodeToBooleanCodec codec = new JsonNodeToBooleanCodec(inputs);

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.booleanNode(true))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.booleanNode(false))
        .to(false)
        .convertsFrom(JsonNodeFactory.instance.textNode("FOO"))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.textNode("BAR"))
        .to(false)
        .convertsFrom(JsonNodeFactory.instance.textNode("foo"))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.textNode("bar"))
        .to(false)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.nullNode())
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(true)
        .from(JsonNodeFactory.instance.booleanNode(true))
        .convertsTo(false)
        .from(JsonNodeFactory.instance.booleanNode(false))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid boolean"));
  }
}
