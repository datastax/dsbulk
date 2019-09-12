/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockTupleType;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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
    tupleType = mockTupleType(V4, CodecRegistry.DEFAULT, DataTypes.TIMESTAMP, DataTypes.TEXT);
    codec1 =
        (StringToTupleCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL", "")
                .build()
                .codecFor(tupleType, GenericType.STRING);
    codec2 =
        (StringToTupleCodec)
            new ExtendedCodecRegistryBuilder()
                .allowExtraFields(true)
                .build()
                .codecFor(tupleType, GenericType.STRING);
    codec3 =
        (StringToTupleCodec)
            new ExtendedCodecRegistryBuilder()
                .allowMissingFields(true)
                .build()
                .codecFor(tupleType, GenericType.STRING);
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
