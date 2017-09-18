/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.config;

public class BulkConfigurationException extends Exception {
  private String path;

  public BulkConfigurationException(String message, String path) {
    super(message);
    this.path = path;
  }

  public BulkConfigurationException(String message) {
    super(message);
  }

  public String getPath() {
    return path;
  }
}
