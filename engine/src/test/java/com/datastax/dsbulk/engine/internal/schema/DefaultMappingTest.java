/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.commons.codecs.writetime.WriteTimeCodec;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DefaultMappingTest {

  @Test
  void should_create_mapping() {
    ImmutableSetMultimap<Field, CQLWord> fieldsToVariables =
        ImmutableSetMultimap.<Field, CQLWord>builder()
            .put(new DefaultMappedField("f1"), CQLWord.fromInternal("c1"))
            .build();
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);

    when(extendedCodecRegistry.codecFor(DataTypes.TEXT, GenericType.STRING))
        .thenReturn(TypeCodecs.TEXT);
    DefaultMapping mapping =
        new DefaultMapping(fieldsToVariables, extendedCodecRegistry, ImmutableSet.of());
    assertThat(mapping.fieldToVariables(new MappedMappingField("f1")))
        .containsExactly(CQLWord.fromInternal("c1"));
    assertThat(mapping.fieldToVariables(new MappedMappingField("nonexistent"))).isEmpty();
    assertThat(mapping.variableToFields(CQLWord.fromInternal("c1")))
        .containsExactly(new MappedMappingField("f1"));
    assertThat(mapping.variableToFields(CQLWord.fromInternal("nonexistent"))).isEmpty();
    assertThat(mapping.codec(CQLWord.fromInternal("f1"), DataTypes.TEXT, GenericType.STRING))
        .isInstanceOf(TypeCodecs.TEXT.getClass());
  }

  @Test
  void should_detect_writetime_variable() {
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);
    ConvertingCodec<String, Instant> codec = new StringToInstantCodec(null, null, null, null);
    when(extendedCodecRegistry.<String, Instant>convertingCodecFor(
            DataTypes.TIMESTAMP, GenericType.STRING))
        .thenReturn(codec);
    DefaultMapping mapping =
        new DefaultMapping(
            ImmutableSetMultimap.of(),
            extendedCodecRegistry,
            ImmutableSet.of(CQLWord.fromInternal("myWriteTimeVar")));
    assertThat(
            mapping.codec(
                CQLWord.fromInternal("myWriteTimeVar"), DataTypes.BIGINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(WriteTimeCodec.class);
  }
}
