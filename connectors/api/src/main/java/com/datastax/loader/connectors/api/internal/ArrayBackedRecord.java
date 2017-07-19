/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.Record;
import com.google.common.collect.AbstractIterator;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** */
public class ArrayBackedRecord implements Record {

  private final Object[] array;

  public ArrayBackedRecord(Object[] array) {
    this.array = array;
  }

  @Override
  public int size() {
    return array.length;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof Integer) {
      int i = (Integer) key;
      return i >= 0 && i < size();
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    for (Object elt : array) {
      if (Objects.equals(elt, value)) return true;
    }
    return false;
  }

  @Override
  public Object get(Object key) {
    if (containsKey(key)) {
      int i = (Integer) key;
      return array[i];
    }
    return null;
  }

  @Override
  public Object put(Object key, Object value) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public Object remove(Object key) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public void putAll(Map<?, ?> m) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public Set<Object> keySet() {
    return IntStream.range(0, size()).boxed().collect(Collectors.toSet());
  }

  @Override
  public Collection<Object> values() {
    return Arrays.asList(array);
  }

  @Override
  public Set<Entry<Object, Object>> entrySet() {
    return new AbstractSet<Entry<Object, Object>>() {

      @Override
      public Iterator<Entry<Object, Object>> iterator() {

        return new AbstractIterator<Entry<Object, Object>>() {
          int index = 0;

          @Override
          protected Entry<Object, Object> computeNext() {
            if (containsKey(index)) return new AbstractMap.SimpleEntry<>(index, array[index++]);
            return endOfData();
          }
        };
      }

      @Override
      public int size() {
        return ArrayBackedRecord.this.size();
      }
    };
  }
}
