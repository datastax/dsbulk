/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import java.util.List;

/** */
public class SSLOptions {

  private String sslEngineFactory;

  private List<String> cipherSuites;

  public void configure(Cluster.Builder builder) {
    // TODO
  }

  public String getSslEngineFactory() {
    return sslEngineFactory;
  }

  public void setSslEngineFactory(String sslEngineFactory) {
    this.sslEngineFactory = sslEngineFactory;
  }

  public List<String> getCipherSuites() {
    return cipherSuites;
  }

  public void setCipherSuites(List<String> cipherSuites) {
    this.cipherSuites = cipherSuites;
  }
}
