/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.loader.connectors.api.Record;
import com.google.common.base.MoreObjects;
import java.net.URL;

/** */
public class UnmappableStatement extends SimpleStatement {

  private final URL location;
  private final Record record;
  private final Throwable error;

  public UnmappableStatement(URL location, Record record, Throwable error) {
    super(error.getMessage());
    this.location = location;
    this.record = record;
    this.error = error;
  }

  public URL getLocation() {
    return location;
  }

  public Record getRecord() {
    return record;
  }

  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("record", record).add("error", error).toString();
  }
}
