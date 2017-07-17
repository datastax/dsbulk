/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import java.time.Duration;

/** */
@SuppressWarnings("unused")
public class SocketOptions {

  private Duration readTimeout;

  public void configure(Cluster.Builder builder) {
    builder.withSocketOptions(
        new com.datastax.driver.core.SocketOptions()
            .setReadTimeoutMillis((int) readTimeout.toMillis()));
  }

  public Duration getReadTimeout() {
    return readTimeout;
  }

  public void setReadTimeout(Duration readTimeout) {
    this.readTimeout = readTimeout;
  }
}
