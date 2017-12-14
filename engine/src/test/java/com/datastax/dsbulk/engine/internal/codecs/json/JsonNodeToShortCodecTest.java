/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class JsonNodeToShortCodecTest {

  private final JsonNodeToShortCodec codec =
      new JsonNodeToShortCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode((short) 0))
        .to((short) 0)
        .convertsFrom(JsonNodeFactory.instance.numberNode((short) 32767))
        .to((short) 32767)
        .convertsFrom(JsonNodeFactory.instance.numberNode((short) -32768))
        .to((short) -32768)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to((short) 0)
        .convertsFrom(JsonNodeFactory.instance.textNode("32767"))
        .to((short) 32767)
        .convertsFrom(JsonNodeFactory.instance.textNode("-32768"))
        .to((short) -32768)
        .convertsFrom(JsonNodeFactory.instance.textNode("32,767"))
        .to((short) 32767)
        .convertsFrom(JsonNodeFactory.instance.textNode("-32,768"))
        .to((short) -32768)
        .convertsFrom(JsonNodeFactory.instance.textNode("1970-01-01T00:00:00Z"))
        .to((short) 0)
        .convertsFrom(JsonNodeFactory.instance.textNode("TRUE"))
        .to((short) 1)
        .convertsFrom(JsonNodeFactory.instance.textNode("FALSE"))
        .to((short) 0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo((short) 0)
        .from(JsonNodeFactory.instance.numberNode((short) 0))
        .convertsTo(Short.MAX_VALUE)
        .from(JsonNodeFactory.instance.numberNode((short) 32_767))
        .convertsTo(Short.MIN_VALUE)
        .from(JsonNodeFactory.instance.numberNode((short) -32_768))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid short"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("1.2"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("32768"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("-32769"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
