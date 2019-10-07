/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.collection;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class CollectionToCollectionCodecTest {
  private final ExtendedCodecRegistry codecRegistry = new ExtendedCodecRegistryBuilder().build();
  private final CollectionToCollectionCodec listCodec =
      (CollectionToCollectionCodec)
          codecRegistry.convertingCodecFor(
              DataTypes.listOf(DataTypes.SMALLINT), GenericType.listOf(Integer.class));
  private final CollectionToCollectionCodec setCodec =
      (CollectionToCollectionCodec)
          codecRegistry.convertingCodecFor(
              DataTypes.listOf(DataTypes.TEXT), GenericType.setOf(Integer.class));
  private final List<Integer> external = Arrays.asList(37, 49, 12);

  @Test
  void should_convert_when_valid_input() {
    // Convert List<Integer> to List<Short>
    List<Short> shortInternal = Arrays.asList((short) 37, (short) 49, (short) 12);
    assertThat(listCodec)
        .convertsFromExternal(external)
        .toInternal(shortInternal)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    // Convert Set<Integer> to List<String>
    Set<Integer> setExternal = new TreeSet<>(external);
    List<String> stringInternal = Arrays.asList("12", "37", "49");
    assertThat(setCodec).convertsFromExternal(setExternal).toInternal(stringInternal);
  }
}
