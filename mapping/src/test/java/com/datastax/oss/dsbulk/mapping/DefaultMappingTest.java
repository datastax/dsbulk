/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
