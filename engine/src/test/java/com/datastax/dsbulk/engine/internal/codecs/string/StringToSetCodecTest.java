/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.set;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Set;
import org.junit.Test;

public class StringToSetCodecTest {

  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  private JsonNodeToStringCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  private TypeCodec<Set<Double>> setCodec1 = set(cdouble());
  private TypeCodec<Set<String>> setCodec2 = set(varchar());

  private StringToSetCodec<Double> codec1 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec1, eltCodec1, objectMapper), objectMapper);
  private StringToSetCodec<String> codec2 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec2, eltCodec2, objectMapper), objectMapper);

  @Test
  public void should_convert_from_valid_input() throws Exception {
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
  public void should_convert_to_valid_input() throws Exception {
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
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec1).cannotConvertFrom("1,not a valid double");
  }
}
