/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToTupleCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToTupleCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final CodecRegistry codecRegistry = new CodecRegistry().register(InstantCodec.instance);
  private final TupleType tupleType = newTupleType(V4, codecRegistry, timestamp(), varchar());

  private final ConvertingCodec eltCodec1 =
      new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
  private final ConvertingCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  @SuppressWarnings("unchecked")
  private final List<ConvertingCodec<JsonNode, Object>> eltCodecs =
      Lists.newArrayList(eltCodec1, eltCodec2);

  private final StringToTupleCodec codec =
      new StringToTupleCodec(
          new JsonNodeToTupleCodec(TypeCodec.tuple(tupleType), eltCodecs, objectMapper),
          objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("[\"2016-07-24T20:34:12.999\",\"+01:00\"]")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom("['2016-07-24T20:34:12.999','+01:00']")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom("[\"\",\"\"]")
        .to(tupleType.newValue(null, ""))
        .convertsFrom("[null,null]")
        .to(tupleType.newValue(null, null))
        .convertsFrom("[,]")
        .to(tupleType.newValue(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .from("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .convertsTo(tupleType.newValue(null, null))
        .from("[null,null]")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("[\"not a valid tuple\"]")
        .cannotConvertFrom("{\"not a valid tuple\":42}")
        .cannotConvertFrom("[\"2016-07-24T20:34:12.999\"]");
  }
}
