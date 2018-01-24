/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
