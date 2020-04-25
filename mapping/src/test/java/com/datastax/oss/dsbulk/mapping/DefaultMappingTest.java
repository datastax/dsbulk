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
package com.datastax.oss.dsbulk.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.writetime.WriteTimeCodec;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Field;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultMappingTest {

  @Mock ConvertingCodecFactory codecFactory;
  @Mock ConvertingCodec<String, Instant> instantCodec;
  @Mock ConvertingCodec<String, String> textCodec;
  @Mock ConvertingCodec<String, Boolean> booleanCodec;

  @Test
  void should_create_mapping() {

    ImmutableSetMultimap<Field, CQLWord> fieldsToVariables =
        ImmutableSetMultimap.<Field, CQLWord>builder()
            .put(new DefaultMappedField("f1"), CQLWord.fromInternal("c1"))
            .put(new DefaultMappedField("f2"), CQLWord.fromInternal("c2"))
            .build();

    when(codecFactory.createConvertingCodec(DataTypes.TEXT, GenericType.STRING, true))
        .thenAnswer(invocation -> textCodec);
    when(codecFactory.createConvertingCodec(DataTypes.BOOLEAN, GenericType.STRING, true))
        .thenAnswer(invocation -> booleanCodec);

    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, codecFactory, ImmutableSet.of());

    assertThat(mapping.fieldToVariables(new MappedMappingField("f1")))
        .containsExactly(CQLWord.fromInternal("c1"));
    assertThat(mapping.fieldToVariables(new MappedMappingField("f2")))
        .containsExactly(CQLWord.fromInternal("c2"));
    assertThat(mapping.fieldToVariables(new MappedMappingField("nonexistent"))).isEmpty();

    assertThat(mapping.variableToFields(CQLWord.fromInternal("c1")))
        .containsExactly(new MappedMappingField("f1"));
    assertThat(mapping.variableToFields(CQLWord.fromInternal("c2")))
        .containsExactly(new MappedMappingField("f2"));
    assertThat(mapping.variableToFields(CQLWord.fromInternal("nonexistent"))).isEmpty();

    assertThat(mapping.codec(CQLWord.fromInternal("f1"), DataTypes.TEXT, GenericType.STRING))
        .isSameAs(textCodec);
    assertThat(mapping.codec(CQLWord.fromInternal("f2"), DataTypes.BOOLEAN, GenericType.STRING))
        .isSameAs(booleanCodec);
  }

  @Test
  void should_detect_writetime_variable() {

    when(codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, true))
        .thenAnswer(invocation -> instantCodec);

    DefaultMapping mapping =
        new DefaultMapping(
            ImmutableSetMultimap.of(),
            codecFactory,
            ImmutableSet.of(CQLWord.fromInternal("myWriteTimeVar")));

    assertThat(
            mapping.codec(
                CQLWord.fromInternal("myWriteTimeVar"), DataTypes.BIGINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(WriteTimeCodec.class);
  }
}
