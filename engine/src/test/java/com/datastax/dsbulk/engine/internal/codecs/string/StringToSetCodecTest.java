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
import static org.assertj.core.util.Sets.newLinkedHashSet;

import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToSetCodecTest {

  private StringToSetCodec<Double> codec1;
  private StringToSetCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry = newCodecRegistry("nullStrings = [NULL]");
    codec1 =
        (StringToSetCodec<Double>)
            codecRegistry.codecFor(DataTypes.setOf(DataTypes.DOUBLE), GenericType.STRING);
    codec2 =
        (StringToSetCodec<String>)
            codecRegistry.codecFor(DataTypes.setOf(DataTypes.TEXT), GenericType.STRING);
  }

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
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(newLinkedHashSet())
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
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal("[\"\",\"\"]")
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal("['','']")
        .toInternal(newLinkedHashSet(""))
        .convertsFromExternal("[\"NULL\",\"NULL\"]")
        .toInternal(newLinkedHashSet("NULL"))
        .convertsFromExternal("['NULL','NULL']")
        .toInternal(newLinkedHashSet("NULL"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("[]")
        .toInternal(newLinkedHashSet())
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
        .convertsFromInternal(newLinkedHashSet())
        .toExternal("[]")
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
        .convertsFromInternal(newLinkedHashSet(""))
        .toExternal("[\"\"]")
        .convertsFromInternal(newLinkedHashSet((String) null))
        .toExternal("[null]")
        .convertsFromInternal(newLinkedHashSet())
        .toExternal("[]")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1)
        .cannotConvertFromExternal("[1,\"not a valid double\"]")
        .cannotConvertFromExternal("{ \"not a valid array\" : 42 }")
        .cannotConvertFromExternal("42");
  }
}
