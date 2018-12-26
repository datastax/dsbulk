/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.timestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.writetime.WriteTimeCodec;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DefaultMappingTest {

  @Test
  void should_create_mapping() {
    ImmutableMultimap<Field, CQLFragment> fieldsToVariables =
        ImmutableMultimap.<Field, CQLFragment>builder()
            .put(new DefaultMappedField("f1"), CQLIdentifier.fromInternal("c1"))
            .build();
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);
    when(extendedCodecRegistry.codecFor(DataType.varchar(), TypeToken.of(String.class)))
        .thenReturn(TypeCodec.varchar());
    DefaultMapping mapping =
        new DefaultMapping(fieldsToVariables, extendedCodecRegistry, ImmutableSet.of());
    assertThat(mapping.fieldToVariables(new MappedMappingField("f1")))
        .containsExactly(CQLIdentifier.fromInternal("c1"));
    assertThat(mapping.fieldToVariables(new MappedMappingField("nonexistent"))).isEmpty();
    assertThat(mapping.variableToFields(CQLIdentifier.fromInternal("c1")))
        .containsExactly(new MappedMappingField("f1"));
    assertThat(mapping.variableToFields(CQLIdentifier.fromInternal("nonexistent"))).isEmpty();
    assertThat(
            mapping.codec(
                CQLIdentifier.fromInternal("f1"), DataType.varchar(), TypeToken.of(String.class)))
        .isInstanceOf(TypeCodec.varchar().getClass());
  }

  @Test
  void should_detect_writetime_variable() {
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);
    ConvertingCodec<String, Instant> codec = new StringToInstantCodec(null, null, null, null);
    when(extendedCodecRegistry.<String, Instant>convertingCodecFor(
            timestamp(), TypeToken.of(String.class)))
        .thenReturn(codec);
    DefaultMapping mapping =
        new DefaultMapping(
            ImmutableMultimap.of(),
            extendedCodecRegistry,
            ImmutableSet.of(CQLIdentifier.fromInternal("myWriteTimeVar")));
    assertThat(
            mapping.codec(
                CQLIdentifier.fromInternal("myWriteTimeVar"),
                DataType.bigint(),
                TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(WriteTimeCodec.class);
  }
}
