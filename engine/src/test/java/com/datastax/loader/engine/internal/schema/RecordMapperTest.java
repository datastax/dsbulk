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
import com.datastax.loader.connectors.api.MappedRecord;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RecordMapperTest {

  private Mapping mapping;
  private MappedRecord record;
  private BoundStatement boundStatement;

  @Before
  public void setUp() throws Exception {
    boundStatement = mock(BoundStatement.class);
    mapping = mock(Mapping.class);
    record = mock(MappedRecord.class);

    when(record.fields()).thenReturn(new HashSet<>(Arrays.asList(0, 1)));
    when(record.getFieldValue(0)).thenReturn(42);

    when(mapping.map(0)).thenReturn("f0");
    when(mapping.map(1)).thenReturn("f1");
  }

  @Test
  public void should_map_string_int_columns() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(null, mapping, null, true, (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    ArgumentCaptor<String> variable = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Class> aClass = ArgumentCaptor.forClass(Class.class);

    //noinspection unchecked
    verify(boundStatement, times(2)).set(variable.capture(), value.capture(), aClass.capture());

    assertThat(variable.getAllValues().get(0)).isEqualTo("f0");
    assertThat(value.getAllValues().get(0)).isEqualTo(42);
    assertThat(aClass.getAllValues().get(0)).isSameAs(Integer.class);

    assertThat(variable.getAllValues().get(1)).isEqualTo("f1");
    assertThat(value.getAllValues().get(1)).isEqualTo("NIL");
    assertThat(aClass.getAllValues().get(1)).isSameAs(String.class);
  }

  @Test
  public void should_map_null_to_unset() throws Exception {
    when(record.getFieldValue(1)).thenReturn(null);
    RecordMapper mapper =
        new RecordMapper(null, mapping, null, true, (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    ArgumentCaptor<String> variable = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Class> aClass = ArgumentCaptor.forClass(Class.class);

    //noinspection unchecked
    verify(boundStatement).set(variable.capture(), value.capture(), aClass.capture());

    assertThat(variable.getValue()).isEqualTo("f0");
    assertThat(value.getValue()).isEqualTo(42);
    assertThat(aClass.getValue()).isSameAs(Integer.class);
  }

  @Test
  public void should_map_null_word_to_unset() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(null, mapping, "NIL", true, (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    ArgumentCaptor<String> variable = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Class> aClass = ArgumentCaptor.forClass(Class.class);

    //noinspection unchecked
    verify(boundStatement).set(variable.capture(), value.capture(), aClass.capture());

    assertThat(variable.getValue()).isEqualTo("f0");
    assertThat(value.getValue()).isEqualTo(42);
    assertThat(aClass.getValue()).isSameAs(Integer.class);
  }

  @Test
  public void should_map_null_to_null() throws Exception {
    when(record.getFieldValue(1)).thenReturn(null);
    RecordMapper mapper =
        new RecordMapper(null, mapping, null, false, (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    ArgumentCaptor<String> variable = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Class> aClass = ArgumentCaptor.forClass(Class.class);

    //noinspection unchecked
    verify(boundStatement).set(variable.capture(), value.capture(), aClass.capture());

    assertThat(variable.getValue()).isEqualTo("f0");
    assertThat(value.getValue()).isEqualTo(42);
    assertThat(aClass.getValue()).isSameAs(Integer.class);

    verify(boundStatement).setToNull(variable.capture());
    assertThat(variable.getValue()).isEqualTo("f1");
  }

  @Test
  public void should_map_null_word_to_null() throws Exception {
    when(record.getFieldValue(1)).thenReturn("NIL");
    RecordMapper mapper =
        new RecordMapper(null, mapping, "NIL", false, (mappedRecord, statement) -> boundStatement);
    Statement result = mapper.map(record);
    assertThat(result).isSameAs(boundStatement);

    ArgumentCaptor<String> variable = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Class> aClass = ArgumentCaptor.forClass(Class.class);

    //noinspection unchecked
    verify(boundStatement).set(variable.capture(), value.capture(), aClass.capture());

    assertThat(variable.getValue()).isEqualTo("f0");
    assertThat(value.getValue()).isEqualTo(42);
    assertThat(aClass.getValue()).isSameAs(Integer.class);

    verify(boundStatement).setToNull(variable.capture());
    assertThat(variable.getValue()).isEqualTo("f1");
  }
}
