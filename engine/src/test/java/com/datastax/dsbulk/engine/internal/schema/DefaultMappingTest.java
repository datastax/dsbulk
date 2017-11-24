/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.Test;

class DefaultMappingTest {

  @Test
  void should_create_mapping() throws Exception {
    ImmutableBiMap<String, String> fieldsToVariables =
        ImmutableBiMap.<String, String>builder().put("f1", "c1").build();
    ExtendedCodecRegistry extendedCodecRegistry = mock(ExtendedCodecRegistry.class);
    when(extendedCodecRegistry.codecFor(DataType.varchar(), TypeToken.of(String.class)))
        .thenReturn(TypeCodec.varchar());
    DefaultMapping mapping = new DefaultMapping(fieldsToVariables, extendedCodecRegistry);
    assertThat(mapping.fieldToVariable("f1")).isEqualTo("c1");
    assertThat(mapping.fieldToVariable("nonexistent")).isNull();
    assertThat(mapping.variableToField("c1")).isEqualTo("f1");
    assertThat(mapping.variableToField("nonexistent")).isNull();
    assertThat(mapping.codec("f1", DataType.varchar(), TypeToken.of(String.class)))
        .isInstanceOf(TypeCodec.varchar().getClass());
  }
}
