/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.dsbulk.connectors.api.Record;
import com.google.common.base.MoreObjects;
import java.net.URI;
import java.util.function.Supplier;

/** */
public class UnmappableStatement extends BulkSimpleStatement<Record> {

  private final Supplier<URI> location;
  private final Throwable error;

  public UnmappableStatement(Record record, Supplier<URI> location, Throwable error) {
    super(record, error.getMessage());
    this.location = location;
    this.error = error;
  }

  public URI getLocation() {
    return location.get();
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
