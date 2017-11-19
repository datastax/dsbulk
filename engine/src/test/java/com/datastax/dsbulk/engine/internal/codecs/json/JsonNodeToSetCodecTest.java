/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.set;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonNodeToSetCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  private final JsonNodeToStringCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  private final TypeCodec<Set<Double>> setCodec1 = set(cdouble());
  private final TypeCodec<Set<String>> setCodec2 = set(varchar());

  private final JsonNodeToSetCodec<Double> codec1 =
      new JsonNodeToSetCodec<>(setCodec1, eltCodec1, objectMapper);

  private final JsonNodeToSetCodec<String> codec2 =
      new JsonNodeToSetCodec<>(setCodec2, eltCodec2, objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec1)
        .convertsFrom(objectMapper.readTree("[1,2,3]"))
        .to(newLinkedHashSet(1d, 2d, 3d))
        .convertsFrom(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .to(newLinkedHashSet(1d, 2d, 3d))
        .convertsFrom(objectMapper.readTree("[1234.56,78900]"))
        .to(newLinkedHashSet(1234.56d, 78900d))
        .convertsFrom(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .to(newLinkedHashSet(1234.56d, 78900d))
        .convertsFrom(objectMapper.readTree("[,]"))
        .to(newLinkedHashSet(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
    assertThat(codec2)
        .convertsFrom(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom(objectMapper.readTree("['foo','bar']"))
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .to(newLinkedHashSet("foo", "bar"))
        .convertsFrom(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .to(newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFrom(objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .to(newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFrom(objectMapper.readTree("[,]"))
        .to(newLinkedHashSet(null, null))
        .convertsFrom(objectMapper.readTree("[null,null]"))
        .to(newLinkedHashSet(null, null))
        .convertsFrom(objectMapper.readTree("[\"\",\"\"]"))
        .to(newLinkedHashSet("", ""))
        .convertsFrom(objectMapper.readTree("['','']"))
        .to(newLinkedHashSet("", ""))
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
        .convertsTo(newLinkedHashSet(1d, 2d, 3d))
        .from(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsTo(newLinkedHashSet(1234.56d, 78900d))
        .from(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsTo(newLinkedHashSet(1d, null))
        .from(objectMapper.readTree("[1.0,null]"))
        .convertsTo(newLinkedHashSet(null, 0d))
        .from(objectMapper.readTree("[null,0.0]"))
        .convertsTo(newLinkedHashSet((Double) null))
        .from(objectMapper.readTree("[null]"))
        .convertsTo(null)
        .from(objectMapper.getNodeFactory().nullNode());
    assertThat(codec2)
        .convertsTo(newLinkedHashSet("foo", "bar"))
        .from(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsTo(newLinkedHashSet("\"foo\"", "\"bar\""))
        .from(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsTo(newLinkedHashSet("\\foo\\", "\\bar\\"))
        .from(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsTo(newLinkedHashSet(",foo,", ",bar,"))
        .from(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsTo(newLinkedHashSet((String) null))
        .from(objectMapper.readTree("[null]"))
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
