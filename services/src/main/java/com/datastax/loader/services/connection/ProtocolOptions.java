/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;

/** */
@SuppressWarnings("unused")
public class ProtocolOptions {

  private ProtocolVersion version;

  private com.datastax.driver.core.ProtocolOptions.Compression compression;

  public void configure(Cluster.Builder builder) {
    builder.withProtocolVersion(version).withCompression(compression);
  }

  public ProtocolVersion getVersion() {
    return version;
  }

  public void setVersion(ProtocolVersion version) {
    this.version = version;
  }

  public com.datastax.driver.core.ProtocolOptions.Compression getCompression() {
    return compression;
  }

  public void setCompression(com.datastax.driver.core.ProtocolOptions.Compression compression) {
    this.compression = compression;
  }
}
