/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.loader.connectors.api.MappedRecord;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RecordMapperTest {

  private Mapping mapping;
  private MappedRecord record;
  private BoundStatement boundStatement;
  private ArgumentCaptor<String> variableCaptor;
  private ArgumentCaptor<Object> valueCaptor;
  private ArgumentCaptor<TypeCodec> codecCaptor;

  @Before
  public void setUp() throws Exception {
    variableCaptor = ArgumentCaptor.forClass(String.class);
    valueCaptor = ArgumentCaptor.forClass(Object.class);
    codecCaptor = ArgumentCaptor.forClass(TypeCodec.class);

    boundStatement = mock(BoundStatement.class);
    mapping = mock(Mapping.class);
    record = mock(MappedRecord.class);

    when(record.fields()).thenReturn(Sets.newHashSet(0, 1, 2));
    when(record.getFieldValue(0)).thenReturn(42);
    when(record.getFieldValue(2)).thenReturn("NULL");

    when(mapping.map(0)).thenReturn("f0");
    when(mapping.map(1)).thenReturn("f1");
    when(mapping.map(2)).thenReturn("f2");

    TypeCodec codec1 = TypeCodec.cint();
    TypeCodec codec2 = TypeCodec.varchar();

    //noinspection unchecked
    when(mapping.codec("f0", 42)).thenReturn(codec1);
    //noinspection unchecked
    when(mapping.codec("f1", "NIL")).thenReturn(codec2);
    //noinspection unchecked
    when(mapping.codec("f2", "NULL")).thenReturn(codec2);
  }

  @Test
  public void should_map_string_int_columns() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(
            null,
            mapping,
            Collections.emptyList(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    //noinspection unchecked
    verify(boundStatement, times(3))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, "f0", 42, TypeCodec.cint());
    assertParameter(1, "f1", "NIL", TypeCodec.varchar());
    assertParameter(2, "f2", "NULL", TypeCodec.varchar());
  }

  @Test
  public void should_map_null_to_unset() throws Exception {
    when(record.getFieldValue(1)).thenReturn(null);
    RecordMapper mapper =
        new RecordMapper(
            null,
            mapping,
            Collections.emptyList(),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    //noinspection unchecked
    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, "f0", 42, TypeCodec.cint());
    assertParameter(1, "f2", "NULL", TypeCodec.varchar());
  }

  @Test
  public void should_map_null_words_to_unset() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(
            null,
            mapping,
            Arrays.asList("NIL", "NULL"),
            true,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    //noinspection unchecked
    verify(boundStatement)
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, "f0", 42, TypeCodec.cint());
  }

  @Test
  public void should_map_null_to_null() throws Exception {
    when(record.getFieldValue(1)).thenReturn(null);
    RecordMapper mapper =
        new RecordMapper(
            null,
            mapping,
            Collections.emptyList(),
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    //noinspection unchecked
    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, "f0", 42, TypeCodec.cint());
    assertParameter(1, "f2", "NULL", TypeCodec.varchar());

    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo("f1");
  }

  @Test
  public void should_map_null_word_to_null() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(
            null,
            mapping,
            Collections.singletonList("NIL"),
            false,
            (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    //noinspection unchecked
    verify(boundStatement, times(2))
        .set(variableCaptor.capture(), valueCaptor.capture(), codecCaptor.capture());

    assertParameter(0, "f0", 42, TypeCodec.cint());
    assertParameter(1, "f2", "NULL", TypeCodec.varchar());

    verify(boundStatement).setToNull(variableCaptor.capture());
    assertThat(variableCaptor.getValue()).isEqualTo("f1");
  }

  private void assertParameter(
      int index, String expectedVariable, Object expectedValue, TypeCodec<?> expectedCodec) {
    assertThat(variableCaptor.getAllValues().get(index)).isEqualTo(expectedVariable);
    assertThat(valueCaptor.getAllValues().get(index)).isEqualTo(expectedValue);
    assertThat(codecCaptor.getAllValues().get(index)).isSameAs(expectedCodec);
  }
}
