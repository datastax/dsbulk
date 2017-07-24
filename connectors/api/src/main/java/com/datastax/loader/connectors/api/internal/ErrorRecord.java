/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.FailedRecord;
import com.google.common.base.MoreObjects;
import java.net.URL;

/** */
public class ErrorRecord implements FailedRecord {

  private final Object source;
  private final URL location;
  private final Throwable error;

  public ErrorRecord(Object source, URL location, Throwable error) {
    this.source = source;
    this.error = error;
    this.location = location;
  }

  @Override
  public Object getSource() {
    return source;
  }

  @Override
  public Throwable getError() {
    return error;
  }

  @Override
  public URL getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("location", location)
        .add("error", error)
        .toString();
  }
}
