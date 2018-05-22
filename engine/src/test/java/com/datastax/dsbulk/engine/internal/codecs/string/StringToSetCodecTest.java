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
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CqlTemporalFormat;
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

class StringToSetCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private final JsonNodeToDoubleCodec eltCodec1 =
      new JsonNodeToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CqlTemporalFormat.DEFAULT_INSTANCE,
          UTC,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          nullStrings);

  private final JsonNodeToStringCodec eltCodec2 =
      new JsonNodeToStringCodec(TypeCodec.varchar(), objectMapper, nullStrings);

  private final TypeCodec<Set<Double>> setCodec1 = set(cdouble());
  private final TypeCodec<Set<String>> setCodec2 = set(varchar());

  private final StringToSetCodec<Double> codec1 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec1, eltCodec1, objectMapper, nullStrings),
          objectMapper,
          nullStrings);
  private final StringToSetCodec<String> codec2 =
      new StringToSetCodec<>(
          new JsonNodeToSetCodec<>(setCodec2, eltCodec2, objectMapper, nullStrings),
          objectMapper,
          nullStrings);

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("[1,2,3]")
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal(" [  1 , 2 , 3 ] ")
        .toInternal(newLinkedHashSet(1d, 2d, 3d))
        .convertsFromExternal("[1234.56,78900]")
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal("[\"1,234.56\",\"78,900\"]")
        .toInternal(newLinkedHashSet(1234.56d, 78900d))
        .convertsFromExternal("[,]")
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal("[\"foo\",\"bar\"]")
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal("['foo','bar']")
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal(" [ \"foo\" , \"bar\" ] ")
        .toInternal(newLinkedHashSet("foo", "bar"))
        .convertsFromExternal("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]")
        .toInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .convertsFromExternal("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]")
        .toInternal(newLinkedHashSet("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal("[,]")
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(newLinkedHashSet(null, null))
        .convertsFromExternal("[\"\",\"\"]")
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal("['','']")
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal("[\"NULL\",\"NULL\"]")
        .toInternal(newLinkedHashSet((String) null))
        .convertsFromExternal("['NULL','NULL']")
        .toInternal(newLinkedHashSet((String) null))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(newLinkedHashSet(1d, 2d, 3d))
        .toExternal("[1.0,2.0,3.0]")
        .convertsFromInternal(newLinkedHashSet(1234.56d, 78900d))
        .toExternal("[1234.56,78900.0]")
        .convertsFromInternal(newLinkedHashSet(1d, null))
        .toExternal("[1.0,null]")
        .convertsFromInternal(newLinkedHashSet(null, 0d))
        .toExternal("[null,0.0]")
        .convertsFromInternal(newLinkedHashSet((Double) null))
        .toExternal("[null]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec2)
        .convertsFromInternal(newLinkedHashSet("foo", "bar"))
        .toExternal("[\"foo\",\"bar\"]")
        .convertsFromInternal(newLinkedHashSet("\"foo\"", "\"bar\""))
        .toExternal("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]")
        .convertsFromInternal(newLinkedHashSet("\\foo\\", "\\bar\\"))
        .toExternal("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]")
        .convertsFromInternal(newLinkedHashSet(",foo,", ",bar,"))
        .toExternal("[\",foo,\",\",bar,\"]")
        .convertsFromInternal(newLinkedHashSet((String) null))
        .toExternal("[null]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1).cannotConvertFromExternal("[1,not a valid double]");
  }
}
