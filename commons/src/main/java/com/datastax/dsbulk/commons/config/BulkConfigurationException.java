/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.config;

public class BulkConfigurationException extends RuntimeException {

  private final String path;

  public BulkConfigurationException(String message, String path) {
    super(message);
    this.path = path;
  }

  public BulkConfigurationException(String message, Throwable cause, String path) {
    super(message, cause);
    this.path = path;
  }

  public String getPath() {
    return path;
  }
}
