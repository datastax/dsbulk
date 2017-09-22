/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class JsonNodeToLongCodecTest {

  private JsonNodeToLongCodec codec =
      new JsonNodeToLongCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0L))
        .to(0L)
        .convertsFrom(JsonNodeFactory.instance.numberNode(9_223_372_036_854_775_807L))
        .to(Long.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(-9_223_372_036_854_775_808L))
        .to(Long.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to(0L)
        .convertsFrom(JsonNodeFactory.instance.textNode("9223372036854775807"))
        .to(Long.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-9223372036854775808"))
        .to(Long.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("9,223,372,036,854,775,807"))
        .to(Long.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-9,223,372,036,854,775,808"))
        .to(Long.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0L)
        .from(JsonNodeFactory.instance.numberNode(0L))
        .convertsTo(Long.MAX_VALUE)
        .from(JsonNodeFactory.instance.numberNode(9_223_372_036_854_775_807L))
        .convertsTo(Long.MIN_VALUE)
        .from(JsonNodeFactory.instance.numberNode(-9_223_372_036_854_775_808L))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid long"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("1.2"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("9223372036854775808"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("-9223372036854775809"));
  }
}
