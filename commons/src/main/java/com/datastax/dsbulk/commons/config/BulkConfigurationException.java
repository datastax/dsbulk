/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.config;

/** Thrown when DSBulk fails to configure itself. */
public class BulkConfigurationException extends RuntimeException {

  public BulkConfigurationException(String message) {
    super(message);
  }

  public BulkConfigurationException(Throwable cause) {
    this(cause.getMessage(), cause);
  }

  public BulkConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
