/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.list;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class JsonNodeToListCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  private final JsonNodeToStringCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  private final TypeCodec<List<Double>> listCodec1 = list(cdouble());
  private final TypeCodec<List<String>> listCodec2 = list(varchar());

  private final JsonNodeToListCodec<Double> codec1 =
      new JsonNodeToListCodec<>(listCodec1, eltCodec1, objectMapper);

  private final JsonNodeToListCodec<String> codec2 =
      new JsonNodeToListCodec<>(listCodec2, eltCodec2, objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec1)
        .convertsFrom(objectMapper.readTree("[1,2,3]"))
        .to(newArrayList(1d, 2d, 3d))
        .convertsFrom(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .to(newArrayList(1d, 2d, 3d))
        .convertsFrom(objectMapper.readTree("[1234.56,78900]"))
        .to(newArrayList(1234.56d, 78900d))
        .convertsFrom(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .to(newArrayList(1234.56d, 78900d))
        .convertsFrom(objectMapper.readTree("[,]"))
        .to(newArrayList(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
    assertThat(codec2)
        .convertsFrom(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .to(newArrayList("foo", "bar"))
        .convertsFrom(objectMapper.readTree("['foo','bar']"))
        .to(newArrayList("foo", "bar"))
        .convertsFrom(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .to(newArrayList("foo", "bar"))
        .convertsFrom(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .to(newArrayList("\"foo\"", "\"bar\""))
        .convertsFrom(objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .to(newArrayList("\"fo\\o\"", "\"ba\\r\""))
        .convertsFrom(objectMapper.readTree("[,]"))
        .to(newArrayList(null, null))
        .convertsFrom(objectMapper.readTree("[null,null]"))
        .to(newArrayList(null, null))
        .convertsFrom(objectMapper.readTree("[\"\",\"\"]"))
        .to(newArrayList("", ""))
        .convertsFrom(objectMapper.readTree("['','']"))
        .to(newArrayList("", ""))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree("[]"))
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec1)
        .convertsTo(newArrayList(1d, 2d, 3d))
        .from(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsTo(newArrayList(1234.56d, 78900d))
        .from(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsTo(newArrayList(1d, null))
        .from(objectMapper.readTree("[1.0,null]"))
        .convertsTo(newArrayList(null, 0d))
        .from(objectMapper.readTree("[null,0.0]"))
        .convertsTo(newArrayList(null, null))
        .from(objectMapper.readTree("[null,null]"))
        .convertsTo(null)
        .from(objectMapper.getNodeFactory().nullNode());
    assertThat(codec2)
        .convertsTo(newArrayList("foo", "bar"))
        .from(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsTo(newArrayList("\"foo\"", "\"bar\""))
        .from(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsTo(newArrayList("\\foo\\", "\\bar\\"))
        .from(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsTo(newArrayList(",foo,", ",bar,"))
        .from(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsTo(newArrayList(null, null))
        .from(objectMapper.readTree("[null,null]"))
        .convertsTo(null)
        .from(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec1).cannotConvertFrom(objectMapper.readTree("[1,\"not a valid double\"]"));
    assertThat(codec1).cannotConvertFrom(objectMapper.readTree("{ \"not a valid array\" : 42 }"));
    assertThat(codec1).cannotConvertFrom(objectMapper.readTree("42"));
  }
}
