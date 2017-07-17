/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;

import static com.datastax.loader.services.internal.ReflectionUtils.newInstance;

/** */
@SuppressWarnings("unused")
public class Policies {

  private static final String DEFAULT_RETRY_POLICY_FQDN = DefaultRetryPolicy.class.getName();

  private static final String NO_SPECULATIVE_EXECUTION_POLICY_FQDN =
      NoSpeculativeExecutionPolicy.class.getName();

  private String loadBalancing;

  private String retry;

  private String speculativeExecution;

  public void configure(Cluster.Builder builder) {
    builder.withLoadBalancingPolicy(newInstance(loadBalancing));
    if (retry.equals(DEFAULT_RETRY_POLICY_FQDN))
      builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
    else builder.withRetryPolicy(newInstance(retry));
    if (speculativeExecution.equals(NO_SPECULATIVE_EXECUTION_POLICY_FQDN))
      builder.withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE);
    else builder.withSpeculativeExecutionPolicy(newInstance(speculativeExecution));
  }

  public String getLoadBalancing() {
    return loadBalancing;
  }

  public void setLoadBalancing(String loadBalancing) {
    this.loadBalancing = loadBalancing;
  }

  public String getRetry() {
    return retry;
  }

  public void setRetry(String retry) {
    this.retry = retry;
  }

  public String getSpeculativeExecution() {
    return speculativeExecution;
  }

  public void setSpeculativeExecution(String speculativeExecution) {
    this.speculativeExecution = speculativeExecution;
  }
}
