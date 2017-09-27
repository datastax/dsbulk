/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.list;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import org.junit.Test;

public class StringToListCodecTest {

  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  private JsonNodeToStringCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  private final TypeCodec<List<Double>> listCodec1 = list(cdouble());
  private final TypeCodec<List<String>> listCodec2 = list(varchar());

  private StringToListCodec<Double> codec1 =
      new StringToListCodec<>(
          new JsonNodeToListCodec<>(listCodec1, eltCodec1, objectMapper), objectMapper);

  private StringToListCodec<String> codec2 =
      new StringToListCodec<>(
          new JsonNodeToListCodec<>(listCodec2, eltCodec2, objectMapper), objectMapper);

  @Test
  public void should_convert_from_valid_input() throws Exception {
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
  public void should_convert_to_valid_input() throws Exception {
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
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec1).cannotConvertFrom("[1,\"not a valid double\"]");
  }
}
