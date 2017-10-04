/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.UnmappableRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/** */
public class DefaultUnmappableRecord implements UnmappableRecord {

  private final Object source;
  private final Supplier<URI> location;
  private final Throwable error;

  public DefaultUnmappableRecord(Object source, Supplier<URI> location, Throwable error) {
    this.source = source;
    this.location = location;
    this.error = error;
  }

  @Override
  public Object getSource() {
    return source;
  }

  @Override
  public URI getLocation() {
    return location.get();
  }

  @Override
  public Set<String> fields() {
    return ImmutableSet.of();
  }

  @Override
  public Collection<Object> values() {
    return ImmutableList.of();
  }

  @Override
  public Object getFieldValue(String field) {
    return null;
  }

  @Override
  public Throwable getError() {
    return error;
  }
}
