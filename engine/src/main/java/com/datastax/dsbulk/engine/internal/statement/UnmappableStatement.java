/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.dsbulk.connectors.api.Record;
import com.google.common.base.MoreObjects;
import java.net.URI;

/** */
public class UnmappableStatement extends BulkSimpleStatement<Record> {

  private final URI location;
  private final Throwable error;

  public UnmappableStatement(Record record, Throwable error) {
    super(record, error.getMessage());
    this.location = record.getLocation();
    this.error = error;
  }

  public URI getLocation() {
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
