/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.map;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.api.core.type.DataTypes.SMALLINT;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapToMapCodecTest {

  private final MapToMapCodec<Integer, Integer, Short, Short> codec =
      (MapToMapCodec<Integer, Integer, Short, Short>)
          new ExtendedCodecRegistryBuilder()
              .build()
              .<Map<Integer, Integer>, Map<Short, Short>>convertingCodecFor(
                  DataTypes.mapOf(SMALLINT, SMALLINT),
                  GenericType.mapOf(Integer.class, Integer.class));

  private final Map<Integer, Integer> external =
      ImmutableMap.<Integer, Integer>builder().put(1, 99).put(2, 98).build();

  @Test
  void should_convert_when_valid_input() {
    // Convert Map<Integer, Integer> to Map<Short, Short>
    Map<Short, Short> shortInternal =
        ImmutableMap.<Short, Short>builder()
            .put((short) 1, (short) 99)
            .put((short) 2, (short) 98)
            .build();
    assertThat(codec)
        .convertsFromExternal(external)
        .toInternal(shortInternal)
        .convertsFromInternal(shortInternal)
        .toExternal(external)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
