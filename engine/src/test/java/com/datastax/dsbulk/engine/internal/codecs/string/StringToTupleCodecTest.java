/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToTupleCodecTest {

  private TupleType tupleType;

  private StringToTupleCodec codec;

  @BeforeEach
  void setUp() {
    tupleType =
        newTupleType(V4, new DefaultCodecRegistry("test"), DataTypes.TIMESTAMP, DataTypes.TEXT);

    //        newTupleType(
    //            V4, new CodecRegistry().register(InstantCodec.instance), DataTypes.TIMESTAMP,
    // varchar());
    codec =
        (StringToTupleCodec)
            newCodecRegistry("nullStrings = [NULL, \"\"]")
                .codecFor(tupleType, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\",\"+01:00\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("['2016-07-24T20:34:12.999','+01:00']")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[ \"2016-07-24T20:34:12.999\" , \"+01:00\" ]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[null,\"\"]")
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal("[null,\"NULL\"]")
        .toInternal(tupleType.newValue(null, "NULL"))
        .convertsFromExternal("[null,null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[,]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(
            tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\"]")
        .convertsFromInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), ""))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"\"]")
        .convertsFromInternal(tupleType.newValue(null, ""))
        .toExternal("[null,\"\"]")
        .convertsFromInternal(tupleType.newValue(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("[\"not a valid tuple\"]")
        .cannotConvertFromExternal("{\"not a valid tuple\":42}")
        .cannotConvertFromExternal("[\"2016-07-24T20:34:12.999\"]");
  }
}
