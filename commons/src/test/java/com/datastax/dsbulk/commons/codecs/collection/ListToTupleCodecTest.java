/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.collection;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newTupleType;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListToTupleCodecTest {
  private TupleType tupleType =
      newTupleType(
          V4,
          new DefaultCodecRegistry("test", TypeCodecs.TIMESTAMP),
          DataTypes.TIMESTAMP,
          DataTypes.TEXT);
  private ListToTupleCodec<String> codec =
      (ListToTupleCodec<String>)
          newCodecRegistry("")
              .<List<String>, TupleValue>convertingCodecFor(
                  tupleType, GenericType.listOf(GenericType.STRING));

  @Test
  void should_convert_when_valid_input() {
    TupleValue internal = tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00");
    List<String> external = Arrays.asList("2016-07-24T20:34:12.999Z", "+01:00");
    assertThat(codec)
        .convertsFromExternal(external)
        .toInternal(internal)
        .convertsFromInternal(internal)
        .toExternal(external)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {
    assertThat(codec)
        .cannotConvertFromExternal(Collections.singletonList("2016-07-24T20:34:12.999Z"));
  }
}
