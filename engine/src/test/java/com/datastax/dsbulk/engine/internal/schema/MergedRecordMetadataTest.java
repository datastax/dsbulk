/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecordMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.junit.Test;

public class MergedRecordMetadataTest {

  RecordMetadata fallback =
      new DefaultRecordMetadata(ImmutableMap.of("f1", TypeToken.of(Integer.class)));

  @Test
  public void should_return_null_when_unknown_field() throws Exception {
    ImmutableMap<String, TypeToken<?>> fieldsToTypes = ImmutableMap.of();
    MergedRecordMetadata metadata = new MergedRecordMetadata(fieldsToTypes, fallback);
    TypeToken<?> type = metadata.getFieldType("nonexistent", null);
    assertThat(type).isNull();
  }

  @Test
  public void should_return_field_from_inner_map() throws Exception {
    MergedRecordMetadata metadata =
        new MergedRecordMetadata(ImmutableMap.of("f1", TypeToken.of(String.class)), fallback);
    TypeToken<?> type = metadata.getFieldType("f1", null);
    assertThat(type.getRawType()).isEqualTo(String.class);
  }

  @Test
  public void should_return_field_from_fallback() throws Exception {
    MergedRecordMetadata metadata = new MergedRecordMetadata(ImmutableMap.of(), fallback);
    TypeToken<?> type = metadata.getFieldType("f1", null);
    assertThat(type.getRawType()).isEqualTo(Integer.class);
  }
}
