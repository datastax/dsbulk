/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import java.time.Duration;

/** */
@SuppressWarnings("unused")
public class PoolingOptions {

  private Pool local;

  private Pool remote;

  private Duration heartbeatInterval;

  public void configure(Cluster.Builder builder) {
    com.datastax.driver.core.PoolingOptions poolingOptions =
        new com.datastax.driver.core.PoolingOptions()
            .setCoreConnectionsPerHost(HostDistance.LOCAL, local.getConnections())
            .setMaxConnectionsPerHost(HostDistance.LOCAL, local.getConnections())
            .setMaxRequestsPerConnection(HostDistance.LOCAL, local.getMaxRequestsPerConnection())
            .setCoreConnectionsPerHost(HostDistance.REMOTE, remote.getConnections())
            .setMaxConnectionsPerHost(HostDistance.REMOTE, remote.getConnections())
            .setMaxRequestsPerConnection(HostDistance.REMOTE, remote.getMaxRequestsPerConnection())
            .setHeartbeatIntervalSeconds((int) heartbeatInterval.getSeconds());
    builder.withPoolingOptions(poolingOptions);
  }

  public Pool getLocal() {
    return local;
  }

  public void setLocal(Pool local) {
    this.local = local;
  }

  public Pool getRemote() {
    return remote;
  }

  public void setRemote(Pool remote) {
    this.remote = remote;
  }

  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  /** */
  public static class Pool {

    private int connections;

    private int maxRequestsPerConnection;

    public int getConnections() {
      return connections;
    }

    public void setConnections(int connections) {
      this.connections = connections;
    }

    public int getMaxRequestsPerConnection() {
      return maxRequestsPerConnection;
    }

    public void setMaxRequestsPerConnection(int maxRequestsPerConnection) {
      this.maxRequestsPerConnection = maxRequestsPerConnection;
    }
  }
}
