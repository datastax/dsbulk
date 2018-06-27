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

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.writetime.WriteTimeCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.collect.ImmutableBiMap;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DefaultMappingTest {
  private static final GenericType<String> STRING_TYPE = GenericType.of(String.class);

  @Test
  void should_create_mapping() {
    CqlIdentifier c1 = CqlIdentifier.fromCql("c1");
    ImmutableBiMap<String, CqlIdentifier> fieldsToVariables =
        ImmutableBiMap.<String, CqlIdentifier>builder().put("f1", c1).build();
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);

    when(extendedCodecRegistry.codecFor(DataTypes.TEXT, STRING_TYPE)).thenReturn(TypeCodecs.TEXT);
    DefaultMapping mapping =
        new DefaultMapping(
            fieldsToVariables, extendedCodecRegistry, CqlIdentifier.fromCql("irrelevant"));
    assertThat(mapping.fieldToVariable("f1")).isEqualTo(c1);
    assertThat(mapping.fieldToVariable("nonexistent")).isNull();
    assertThat(mapping.variableToField(c1)).isEqualTo("f1");
    assertThat(mapping.variableToField(CqlIdentifier.fromCql("nonexistent"))).isNull();
    assertThat(mapping.codec(c1, DataTypes.TEXT, STRING_TYPE))
        .isInstanceOf(TypeCodecs.TEXT.getClass());
  }

  @Test
  void should_detect_writetime_variable() {
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);
    ConvertingCodec<String, Instant> codec =
        new StringToInstantCodec(null, null, null, null, null, null);
    when(extendedCodecRegistry.<String, Instant>convertingCodecFor(
            DataTypes.TIMESTAMP, STRING_TYPE))
        .thenReturn(codec);
    CqlIdentifier myWriteTimeVar = CqlIdentifier.fromCql("myWriteTimeVar");
    DefaultMapping mapping =
        new DefaultMapping(ImmutableBiMap.of(), extendedCodecRegistry, myWriteTimeVar);
    assertThat(mapping.codec(myWriteTimeVar, DataTypes.BIGINT, STRING_TYPE))
        .isNotNull()
        .isInstanceOf(WriteTimeCodec.class);
  }
}
