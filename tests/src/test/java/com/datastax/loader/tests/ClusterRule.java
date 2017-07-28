/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests;

import com.datastax.driver.core.*;
import com.datastax.driver.dse.DseCluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;

public class ClusterRule extends CassandraResourceRule {

  // the ccm rule to depend on
  private final CassandraResourceRule cassandraResource;

  // the default cluster that is auto created for this rule.
  private Cluster defaultCluster;

  // the default session that is auto created for this rule and is tied to the given keyspace.
  private Session defaultSession;

  // clusters created by this rule.
  private final Collection<Cluster> clusters = new ArrayList<>();

  private DseCluster.Builder defaultBulder;

  private static final AtomicInteger keyspaceId = new AtomicInteger();

  private final String keyspace = "ks_" + keyspaceId.getAndIncrement();

  private boolean createDefaultSession = true;

  /**
   * Creates a ClusterRule wrapping the provided resource.
   *
   * @param cassandraResource resource to create clusters for.
   * @param builder The config options to pass to the default created cluster.
   */
  public ClusterRule(CassandraResourceRule cassandraResource, DseCluster.Builder builder) {
    this(cassandraResource, true, builder);
  }

  /**
   * Creates a ClusterRule wrapping the provided resource.
   *
   * @param cassandraResource resource to create clusters for.
   * @param createDefaultSession whether or not to create a default session on initialization.
   * @param builder The config options to pass to the default created cluster.
   */
  public ClusterRule(
      CassandraResourceRule cassandraResource,
      boolean createDefaultSession,
      DseCluster.Builder builder) {
    this.cassandraResource = cassandraResource;
    this.defaultBulder = builder;
    this.createDefaultSession = createDefaultSession;
  }

  @Override
  protected void before() {
    // ensure resource is initialized before initializing the defaultCluster.
    defaultCluster = defaultCluster(defaultBulder);
    clusters.add(defaultCluster);

    if (createDefaultSession) {
      if (!(cassandraResource instanceof SimulacronRule)) {
        createKeyspace();
        defaultSession = defaultCluster.connect(keyspace);
      } else {
        defaultSession = defaultCluster.connect();
      }
    }
  }

  private void createKeyspace() {
    try (Session session = newSession()) {
      SimpleStatement createKeyspace =
          new SimpleStatement(
              String.format(
                  "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
                  this.keyspace));
      createKeyspace.setReadTimeoutMillis(30000);
      session.execute(createKeyspace);
    }
  }

  protected void after() {
    this.clusters.forEach(Cluster::close);
  }

  /** @return the default cluster created with this rule. */
  public Cluster cluster() {
    return defaultCluster;
  }

  /** @return the default session created with this rule. */
  public Session session() {
    return defaultSession;
  }

  /** @return keyspace associated with this rule. */
  public String keyspace() {
    return keyspace;
  }

  /**
   * @return A {@link DseCluster.Builder} returns a the provided builder with contact points added
   *     in
   */
  public Cluster defaultCluster(DseCluster.Builder builder) {

    builder.addContactPointsWithPorts(this.getContactPoints());
    return builder.build();
  }

  public static DseCluster.Builder getTestBuilder() {
    DseCluster.Builder clusterBuilder = DseCluster.builder();
    clusterBuilder.withoutJMXReporting();
    clusterBuilder.withoutMetrics();
    clusterBuilder.withCodecRegistry(new CodecRegistry());
    PoolingOptions poolingOptions =
        new PoolingOptions().setConnectionsPerHost(LOCAL, 1, 1).setConnectionsPerHost(REMOTE, 1, 1);
    QueryOptions queryOptions =
        new QueryOptions()
            .setRefreshNodeIntervalMillis(0)
            .setRefreshNodeListIntervalMillis(0)
            .setRefreshSchemaIntervalMillis(0);
    clusterBuilder.withPoolingOptions(poolingOptions);
    clusterBuilder.withQueryOptions(queryOptions);
    return clusterBuilder;
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    return cassandraResource.getHighestProtocolVersion();
  }

  @Override
  public Set<InetSocketAddress> getContactPoints() {
    return cassandraResource.getContactPoints();
  }

  /** @return a new session from the default cluster associated with this rule. */
  public Session newSession() {
    return defaultCluster.connect();
  }
}
