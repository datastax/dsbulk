/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.statement;

import com.datastax.loader.connectors.api.Record;
import com.google.common.base.MoreObjects;
import java.net.URL;

/** */
public class UnmappableStatement extends BulkSimpleStatement<Record> {

  private final URL location;
  private final Throwable error;

  public UnmappableStatement(URL location, Record record, Throwable error) {
    super(record, error.getMessage());
    this.location = location;
    this.error = error;
  }

  public URL getLocation() {
    return location;
  }

  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("record", getSource())
        .add("error", error)
        .toString();
  }
}
