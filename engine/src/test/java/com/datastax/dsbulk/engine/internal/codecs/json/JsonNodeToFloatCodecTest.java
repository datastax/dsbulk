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

class JsonNodeToFloatCodecTest {

  private final JsonNodeToFloatCodec codec =
      new JsonNodeToFloatCodec(
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
        .convertsFrom(JsonNodeFactory.instance.numberNode(0f))
        .to(0f)
        .convertsFrom(JsonNodeFactory.instance.numberNode(1234.56f))
        .to(1234.56f)
        .convertsFrom(JsonNodeFactory.instance.numberNode(3.4028235E38f))
        .to(Float.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(1.4E-45f))
        .to(Float.MIN_VALUE)
        .convertsFrom(
            JsonNodeFactory.instance.numberNode(
                340_282_346_638_528_860_000_000_000_000_000_000_000f))
        .to(Float.MAX_VALUE)
        .convertsFrom(
            JsonNodeFactory.instance.numberNode(0.0000000000000000000000000000000000000000000014f))
        .to(Float.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to(0f)
        .convertsFrom(JsonNodeFactory.instance.textNode("1234.56"))
        .to(1234.56f)
        .convertsFrom(JsonNodeFactory.instance.textNode("1,234.56"))
        .to(1234.56f)
        .convertsFrom(JsonNodeFactory.instance.textNode("3.4028235E38"))
        .to(Float.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("1.4E-45"))
        .to(Float.MIN_VALUE)
        .convertsFrom(
            JsonNodeFactory.instance.textNode(
                "340,282,346,638,528,860,000,000,000,000,000,000,000"))
        .to(Float.MAX_VALUE)
        .convertsFrom(
            JsonNodeFactory.instance.textNode("0.0000000000000000000000000000000000000000000014"))
        .to(Float.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("1970-01-01T00:00:00Z"))
        .to(0f)
        .convertsFrom(JsonNodeFactory.instance.textNode("2000-01-01T00:00:00Z"))
        .to(946684800000f)
        .convertsFrom(JsonNodeFactory.instance.textNode("TRUE"))
        .to(1f)
        .convertsFrom(JsonNodeFactory.instance.textNode("FALSE"))
        .to(0f)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0f)
        .from(JsonNodeFactory.instance.numberNode(0f))
        .convertsTo(1234.56f)
        .from(JsonNodeFactory.instance.numberNode(1234.56f))
        .convertsTo(Float.MAX_VALUE)
        .from(
            JsonNodeFactory.instance.numberNode(
                340_282_346_638_528_860_000_000_000_000_000_000_000f))
        .convertsTo(0.001f)
        .from(JsonNodeFactory.instance.numberNode(0.001f))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid float"));
  }
}
