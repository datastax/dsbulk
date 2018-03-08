/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToTupleCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final CodecRegistry codecRegistry = new CodecRegistry().register(InstantCodec.instance);

  private final TupleType tupleType = newTupleType(V4, codecRegistry, timestamp(), varchar());

  private final List<String> nullWords = newArrayList("NULL");

  private final ConvertingCodec eltCodec1 =
      new JsonNodeToInstantCodec(
          CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC), nullWords);

  private final ConvertingCodec eltCodec2 =
      new JsonNodeToStringCodec(TypeCodec.varchar(), nullWords);

  @SuppressWarnings("unchecked")
  private final List<ConvertingCodec<JsonNode, Object>> eltCodecs =
      Lists.newArrayList(eltCodec1, eltCodec2);

  private final JsonNodeToTupleCodec codec =
      new JsonNodeToTupleCodec(TypeCodec.tuple(tupleType), eltCodecs, objectMapper, nullWords);

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\",\"+01:00\"]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("['2016-07-24T20:34:12.999','+01:00']"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
        .convertsFromInternal(
            tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .toExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .convertsFromInternal(tupleType.newValue(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(null)
        .toExternal(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec)
        .cannotConvertFromExternal(objectMapper.readTree("[\"not a valid tuple\"]"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid tuple\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[\"2016-07-24T20:34:12.999\"]"));
  }
}
