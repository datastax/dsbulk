/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToTupleCodecTest {

  private TupleType tupleType;

  private StringToTupleCodec codec1;
  private StringToTupleCodec codec2;
  private StringToTupleCodec codec3;

  @BeforeEach
  void setUp() {
    tupleType =
        newTupleType(
            V4, new CodecRegistry().register(InstantCodec.instance), timestamp(), varchar());
    codec1 =
        (StringToTupleCodec)
            newCodecRegistry("nullStrings = [NULL, \"\"]")
                .codecFor(tupleType, TypeToken.of(String.class));
    codec2 =
        (StringToTupleCodec)
            newCodecRegistry("", true, false).codecFor(tupleType, TypeToken.of(String.class));
    codec3 =
        (StringToTupleCodec)
            newCodecRegistry("", false, true).codecFor(tupleType, TypeToken.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
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
    // should allow extra elements
    assertThat(codec2)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\",\"+01:00\", 42]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFromExternal("[,\"\",\"\"]")
        .toInternal(tupleType.newValue(null, ""))
        .convertsFromExternal("[null,null,null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[,,]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    // should allow missing elements
    assertThat(codec3)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999\"]")
        .toInternal(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), null))
        .convertsFromExternal("[null]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("[]")
        .toInternal(tupleType.newValue(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
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
    assertThat(codec1).cannotConvertFromExternal("{\"not a valid tuple\":42}");
    // should not allow missing elements
    assertThat(codec2)
        .cannotConvertFromExternal("[\"2016-07-24T20:34:12.999Z\"]")
        .cannotConvertFromExternal("[]");
    // should not allow extra elements
    assertThat(codec3).cannotConvertFromExternal("[\"2016-07-24T20:34:12.999Z\",\"+01:00\",42]");
  }
}
