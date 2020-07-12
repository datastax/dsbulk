/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.jdk.collection;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"unchecked", "rawtypes"})
class CollectionToCollectionCodecTest {

  private final ConvertingCodecFactory codecFactory = new ConvertingCodecFactory();

  private final CollectionToCollectionCodec listCodec =
      (CollectionToCollectionCodec)
          codecFactory.createConvertingCodec(
              DataTypes.listOf(DataTypes.SMALLINT), GenericType.listOf(Integer.class), true);

  private final CollectionToCollectionCodec setCodec =
      (CollectionToCollectionCodec)
          codecFactory.createConvertingCodec(
              DataTypes.listOf(DataTypes.TEXT), GenericType.setOf(Integer.class), true);

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
