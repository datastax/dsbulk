/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
