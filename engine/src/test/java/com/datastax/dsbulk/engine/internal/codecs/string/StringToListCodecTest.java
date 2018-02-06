/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.list;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToListCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  private final JsonNodeToStringCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  private final TypeCodec<List<Double>> listCodec1 = list(cdouble());
  private final TypeCodec<List<String>> listCodec2 = list(varchar());

  private final StringToListCodec<Double> codec1 =
      new StringToListCodec<>(
          new JsonNodeToListCodec<>(listCodec1, eltCodec1, objectMapper), objectMapper);

  private final StringToListCodec<String> codec2 =
      new StringToListCodec<>(
          new JsonNodeToListCodec<>(listCodec2, eltCodec2, objectMapper), objectMapper);

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec1)
        .convertsFrom("[1,2,3]")
        .to(newArrayList(1d, 2d, 3d))
        .convertsFrom(" [  1 , 2 , 3 ] ")
        .to(newArrayList(1d, 2d, 3d))
        .convertsFrom("[1234.56,78900]")
        .to(newArrayList(1234.56d, 78900d))
        .convertsFrom("[\"1,234.56\",\"78,900\"]")
        .to(newArrayList(1234.56d, 78900d))
        .convertsFrom("[,]")
        .to(newArrayList(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    assertThat(codec2)
        .convertsFrom("[\"foo\",\"bar\"]")
        .to(newArrayList("foo", "bar"))
        .convertsFrom("['foo','bar']")
        .to(newArrayList("foo", "bar"))
        .convertsFrom(" [ \"foo\" , \"bar\" ] ")
        .to(newArrayList("foo", "bar"))
        .convertsFrom("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]")
        .to(newArrayList("\"foo\"", "\"bar\""))
        .convertsFrom("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]")
        .to(newArrayList("\"fo\\o\"", "\"ba\\r\""))
        .convertsFrom("[,]")
        .to(newArrayList(null, null))
        .convertsFrom("[null,null]")
        .to(newArrayList(null, null))
        .convertsFrom("[\"\",\"\"]")
        .to(newArrayList("", ""))
        .convertsFrom("['','']")
        .to(newArrayList("", ""))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("[]")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec1)
        .convertsTo(newArrayList(1d, 2d, 3d))
        .from("[1.0,2.0,3.0]")
        .convertsTo(newArrayList(1234.56d, 78900d))
        .from("[1234.56,78900.0]")
        .convertsTo(newArrayList(1d, null))
        .from("[1.0,null]")
        .convertsTo(newArrayList(null, 0d))
        .from("[null,0.0]")
        .convertsTo(newArrayList(null, null))
        .from("[null,null]")
        .convertsTo(null)
        .from(null);
    assertThat(codec2)
        .convertsTo(newArrayList("foo", "bar"))
        .from("[\"foo\",\"bar\"]")
        .convertsTo(newArrayList("\"foo\"", "\"bar\""))
        .from("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]")
        .convertsTo(newArrayList("\\foo\\", "\\bar\\"))
        .from("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]")
        .convertsTo(newArrayList(",foo,", ",bar,"))
        .from("[\",foo,\",\",bar,\"]")
        .convertsTo(newArrayList(null, null))
        .from("[null,null]")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec1).cannotConvertFrom("[1,\"not a valid double\"]");
  }
}
