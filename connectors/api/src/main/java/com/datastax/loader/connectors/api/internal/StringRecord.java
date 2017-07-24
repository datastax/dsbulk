/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.CQLRecord;
import com.google.common.base.MoreObjects;
import java.net.URL;

/** */
public class StringRecord implements CQLRecord {

  private final String source;
  private final URL location;

  public StringRecord(String source, URL location) {
    this.source = source;
    this.location = location;
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
  public String getQueryString() {
    return source;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("location", location)
        .toString();
  }
}
