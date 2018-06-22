/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToListCodecTest {

  private StringToListCodec<Double> codec1;
  private StringToListCodec<Instant> codec2;
  private StringToListCodec<String> codec3;

  private Instant i1 = Instant.parse("2016-07-24T20:34:12.999Z");
  private Instant i2 = Instant.parse("2018-05-25T18:34:12.999Z");

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry = newCodecRegistry("nullStrings = [NULL]");
    codec1 =
        (StringToListCodec<Double>)
            codecRegistry.codecFor(
                DataTypes.listOf(DataTypes.DOUBLE), GenericType.of(String.class));
    codec2 =
        (StringToListCodec<Instant>)
            codecRegistry.codecFor(
                DataTypes.listOf(DataTypes.TIMESTAMP), GenericType.of(String.class));
    codec3 =
        (StringToListCodec<String>)
            codecRegistry.codecFor(DataTypes.listOf(DataTypes.TEXT), GenericType.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("[1,2,3]")
        .toInternal(newArrayList(1d, 2d, 3d))
        .convertsFromExternal(" [  1 , 2 , 3 ] ")
        .toInternal(newArrayList(1d, 2d, 3d))
        .convertsFromExternal("[1234.56,78900]")
        .toInternal(newArrayList(1234.56d, 78900d))
        .convertsFromExternal("[\"1,234.56\",\"78,900\"]")
        .toInternal(newArrayList(1234.56d, 78900d))
        .convertsFromExternal("[,]")
        .toInternal(newArrayList(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal("[\"2016-07-24T20:34:12.999Z\",\"2018-05-25 20:34:12.999+02:00\"]")
        .toInternal(newArrayList(i1, i2))
        .convertsFromExternal("['2016-07-24T20:34:12.999Z','2018-05-25 20:34:12.999+02:00']")
        .toInternal(newArrayList(i1, i2))
        .convertsFromExternal(
            " [ \"2016-07-24T20:34:12.999Z\" , \"2018-05-25 20:34:12.999+02:00\" ] ")
        .toInternal(newArrayList(i1, i2))
        .convertsFromExternal("[,]")
        .toInternal(newArrayList(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(newArrayList(null, null))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(codec3)
        .convertsFromExternal("[\"foo\",\"bar\"]")
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal("['foo','bar']")
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal(" [ \"foo\" , \"bar\" ] ")
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal("[,]")
        .toInternal(newArrayList(null, null))
        .convertsFromExternal("[null,null]")
        .toInternal(newArrayList(null, null))
        .convertsFromExternal("['','']")
        .toInternal(newArrayList("", ""))
        .convertsFromExternal("[\"\",\"\"]")
        .toInternal(newArrayList("", ""))
        .convertsFromExternal("[\"NULL\",\"NULL\"]")
        .toInternal(newArrayList("NULL", "NULL"))
        .convertsFromExternal("['NULL','NULL']")
        .toInternal(newArrayList("NULL", "NULL"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(newArrayList())
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(newArrayList(1d, 2d, 3d))
        .toExternal("[1.0,2.0,3.0]")
        .convertsFromInternal(newArrayList(1234.56d, 78900d))
        .toExternal("[1234.56,78900.0]")
        .convertsFromInternal(newArrayList(1d, null))
        .toExternal("[1.0,null]")
        .convertsFromInternal(newArrayList(null, 0d))
        .toExternal("[null,0.0]")
        .convertsFromInternal(newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec2)
        .convertsFromInternal(newArrayList(i1, i2))
        .toExternal("[\"2016-07-24T20:34:12.999Z\",\"2018-05-25T18:34:12.999Z\"]")
        .convertsFromInternal(newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec3)
        .convertsFromInternal(newArrayList("foo", "bar"))
        .toExternal("[\"foo\",\"bar\"]")
        .convertsFromInternal(newArrayList("", ""))
        .toExternal("[\"\",\"\"]")
        .convertsFromInternal(newArrayList(null, null))
        .toExternal("[null,null]")
        .convertsFromInternal(newArrayList())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1).cannotConvertFromExternal("[1,\"not a valid double\"]");
  }
}
