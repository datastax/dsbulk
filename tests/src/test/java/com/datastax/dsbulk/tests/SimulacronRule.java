/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Collectors;

public class SimulacronRule extends CassandraResourceRule {
  // TODO perhaps share server some other way
  private static final Server server = Server.builder().build();

  private final ClusterSpec clusterSpec;
  private BoundCluster boundCluster;

  public SimulacronRule(ClusterSpec clusterSpec) {
    this.clusterSpec = clusterSpec;
  }

  public SimulacronRule(ClusterSpec.Builder clusterSpec) {
    this(clusterSpec.build());
  }

  /**
   * Convenient fluent name for getting at bound cluster.
   *
   * @return default bound cluster for this simulacron instance.
   */
  public BoundCluster cluster() {
    return boundCluster;
  }

  public BoundCluster getBoundCluster() {
    return boundCluster;
  }

  @Override
  protected void before() {
    boundCluster = server.register(clusterSpec);
  }

  @Override
  protected void after() {
    boundCluster.close();
  }

  /** @return All nodes in first data center. */
  @Override
  public Set<InetSocketAddress> getContactPoints() {
    return boundCluster
        .dc(0)
        .getNodes()
        .stream()
        .map(BoundNode::inetSocketAddress)
        .collect(Collectors.toSet());
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    return ProtocolVersion.V4;
  }
}
