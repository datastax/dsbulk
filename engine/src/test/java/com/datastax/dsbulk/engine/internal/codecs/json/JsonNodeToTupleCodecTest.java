/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.junit.Test;

public class JsonNodeToTupleCodecTest {

  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private CodecRegistry codecRegistry = new CodecRegistry().register(InstantCodec.instance);
  private TupleType tupleType = newTupleType(V4, codecRegistry, timestamp(), varchar());

  private ConvertingCodec eltCodec1 = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT);
  private ConvertingCodec eltCodec2 = new JsonNodeToStringCodec(TypeCodec.varchar());

  @SuppressWarnings("unchecked")
  private List<ConvertingCodec<JsonNode, Object>> eltCodecs =
      Lists.newArrayList(eltCodec1, eltCodec2);

  private JsonNodeToTupleCodec codec =
      new JsonNodeToTupleCodec(TypeCodec.tuple(tupleType), eltCodecs, objectMapper);

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(objectMapper.readTree("[\"2016-07-24T20:34:12.999\",\"+01:00\"]"))
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(objectMapper.readTree("['2016-07-24T20:34:12.999','+01:00']"))
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(objectMapper.readTree("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]"))
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(objectMapper.readTree("[\"\",\"\"]"))
        .to(tupleType.newValue(null, ""))
        .convertsFrom(objectMapper.readTree("[null,null]"))
        .to(tupleType.newValue(null, null))
        .convertsFrom(objectMapper.readTree("[,]"))
        .to(tupleType.newValue(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .from(objectMapper.readTree("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]"))
        .convertsTo(tupleType.newValue(null, null))
        .from(objectMapper.readTree("[null,null]"))
        .convertsTo(null)
        .from(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom(objectMapper.readTree("[\"not a valid tuple\"]"))
        .cannotConvertFrom(objectMapper.readTree("{\"not a valid tuple\":42}"))
        .cannotConvertFrom(objectMapper.readTree("[\"2016-07-24T20:34:12.999\"]"));
  }
}
