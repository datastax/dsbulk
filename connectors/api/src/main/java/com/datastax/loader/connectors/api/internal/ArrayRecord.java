/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.IndexedRecord;
import com.google.common.base.MoreObjects;
import java.net.URL;

/** */
public class ArrayRecord implements IndexedRecord {

  private final Object source;
  private final URL location;
  private final Object[] array;

  public ArrayRecord(Object source, URL location, Object[] array) {
    this.source = source;
    this.location = location;
    this.array = array;
  }

  @Override
  public Object getSource() {
    return source;
  }

  @Override
  public URL getLocation() {
    return location;
  }

  @Override
  public int size() {
    return array.length;
  }

  @Override
  public Object getValue(int i) {
    return array[i];
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("location", location)
        .add("array", array)
        .toString();
  }
}
