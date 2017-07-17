/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;

import static com.datastax.loader.services.internal.ReflectionUtils.newInstance;

/** */
@SuppressWarnings("unused")
public class AuthOptions {

  private String authProvider;

  public void configure(Cluster.Builder builder) {
    if (!authProvider.isEmpty()) builder.withAuthProvider(newInstance(authProvider));
  }

  public String getAuthProvider() {
    return authProvider;
  }

  public void setAuthProvider(String authProvider) {
    this.authProvider = authProvider;
  }
}
