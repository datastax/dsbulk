/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.set;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonNodeToSetCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private final JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          nullStrings);

  private final JsonNodeToStringCodec eltCodec2 =
      new JsonNodeToStringCodec(TypeCodec.varchar(), objectMapper, nullStrings);

  private final TypeCodec<Set<Double>> setCodec1 = set(cdouble());
  private final TypeCodec<Set<String>> setCodec2 = set(varchar());

  private final JsonNodeToSetCodec<Double> codec1 =
      new JsonNodeToSetCodec<>(setCodec1, eltCodec1, objectMapper, nullStrings);

  private final JsonNodeToSetCodec<String> codec2 =
      new JsonNodeToSetCodec<>(setCodec2, eltCodec2, objectMapper, nullStrings);

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree("[1234.56,78900]"))
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("['foo','bar']"))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .toInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFromExternal(
            objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .toInternal(newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("['','']"))
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(newLinkedHashSet((String) null))
        .convertsFromExternal(objectMapper.readTree("['NULL','NULL']"))
        .toInternal(newLinkedHashSet((String) null))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(newLinkedHashSet(1d, 2d, 3d))
        .toExternal(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsFromInternal(newLinkedHashSet(1234.56d, 78900d))
        .toExternal(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsFromInternal(newLinkedHashSet(1d, null))
        .toExternal(objectMapper.readTree("[1.0,null]"))
        .convertsFromInternal(newLinkedHashSet(null, 0d))
        .toExternal(objectMapper.readTree("[null,0.0]"))
        .convertsFromInternal(newLinkedHashSet((Double) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(null)
        .toExternal(objectMapper.getNodeFactory().nullNode());
    assertThat(codec2)
        .convertsFromInternal(newLinkedHashSet("foo", "bar"))
        .toExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsFromInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .toExternal(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsFromInternal(newLinkedHashSet("\\foo\\", "\\bar\\"))
        .toExternal(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsFromInternal(newLinkedHashSet(",foo,", ",bar,"))
        .toExternal(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsFromInternal(newLinkedHashSet((String) null))
        .toExternal(objectMapper.readTree("[null]"))
        .convertsFromInternal(null)
        .toExternal(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid double\"]"));
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("{ \"not a valid array\" : 42 }"));
    assertThat(codec1).cannotConvertFromExternal(objectMapper.readTree("42"));
  }
}
