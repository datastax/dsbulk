/*
 * Copyright DataStax Inc.
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

public class JsonNodeToIntegerCodecTest {

  private JsonNodeToIntegerCodec codec =
      new JsonNodeToIntegerCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0))
        .to(0)
        .convertsFrom(JsonNodeFactory.instance.numberNode(2_147_483_647))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(-2_147_483_648))
        .to(Integer.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to(0)
        .convertsFrom(JsonNodeFactory.instance.textNode("2147483647"))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-2147483648"))
        .to(Integer.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("2,147,483,647"))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-2,147,483,648"))
        .to(Integer.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0)
        .from(JsonNodeFactory.instance.numberNode(0))
        .convertsTo(Integer.MAX_VALUE)
        .from(JsonNodeFactory.instance.numberNode(2_147_483_647))
        .convertsTo(Integer.MIN_VALUE)
        .from(JsonNodeFactory.instance.numberNode(-2_147_483_648))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid integer"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("1.2"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("2147483648"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("-2147483649"));
  }
}
