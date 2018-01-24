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
import static com.datastax.driver.core.TypeCodec.set;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StringToSetCodecTest {

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

  private final TypeCodec<Set<Double>> setCodec1 = set(cdouble());
  private final TypeCodec<Set<String>> setCodec2 = set(varchar());

  private final StringToSetCodec<Double> codec1 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec1, eltCodec1, objectMapper), objectMapper);
  private final StringToSetCodec<String> codec2 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec2, eltCodec2, objectMapper), objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec1)
        .convertsFrom("[1,2,3]")
        .to(newLinkedHashSet(1d, 2d, 3d))
        .convertsFrom(" [  1 , 2 , 3 ] ")
        .to(newLinkedHashSet(1d, 2d, 3d))
        .convertsFrom("[1234.56,78900]")
        .to(newLinkedHashSet(1234.56d, 78900d))
        .convertsFrom("[\"1,234.56\",\"78,900\"]")
        .to(newLinkedHashSet(1234.56d, 78900d))
        .convertsFrom("[,]")
        .to(newLinkedHashSet(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    assertThat(codec2)
        .convertsFrom("[\"foo\",\"bar\"]")
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom("['foo','bar']")
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom(" [ \"foo\" , \"bar\" ] ")
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]")
        .to(newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFrom("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]")
        .to(newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFrom("[,]")
        .to(newLinkedHashSet(null, null))
        .convertsFrom("[null,null]")
        .to(newLinkedHashSet(null, null))
        .convertsFrom("[\"\",\"\"]")
        .to(newLinkedHashSet("", ""))
        .convertsFrom("['','']")
        .to(newLinkedHashSet("", ""))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("[]")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec1)
        .convertsTo(newLinkedHashSet(1d, 2d, 3d))
        .from("[1.0,2.0,3.0]")
        .convertsTo(newLinkedHashSet(1234.56d, 78900d))
        .from("[1234.56,78900.0]")
        .convertsTo(newLinkedHashSet(1d, null))
        .from("[1.0,null]")
        .convertsTo(newLinkedHashSet(null, 0d))
        .from("[null,0.0]")
        .convertsTo(newLinkedHashSet((Double) null))
        .from("[null]")
        .convertsTo(null)
        .from(null);
    assertThat(codec2)
        .convertsTo(newLinkedHashSet("foo", "bar"))
        .from("[\"foo\",\"bar\"]")
        .convertsTo(newLinkedHashSet("\"foo\"", "\"bar\""))
        .from("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]")
        .convertsTo(newLinkedHashSet("\\foo\\", "\\bar\\"))
        .from("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]")
        .convertsTo(newLinkedHashSet(",foo,", ",bar,"))
        .from("[\",foo,\",\",bar,\"]")
        .convertsTo(newLinkedHashSet((String) null))
        .from("[null]")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec1).cannotConvertFrom("[1,not a valid double]");
  }
}
