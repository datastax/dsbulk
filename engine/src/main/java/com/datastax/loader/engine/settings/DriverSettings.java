/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.dse.DseCluster;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.net.InetSocketAddress;

import static com.datastax.loader.engine.internal.ReflectionUtils.newInstance;

/** */
public class DriverSettings {

  private static final String DEFAULT_RETRY_POLICY_FQDN = DefaultRetryPolicy.class.getName();

  private static final String NO_SPECULATIVE_EXECUTION_POLICY_FQDN =
      NoSpeculativeExecutionPolicy.class.getName();

  private final Config config;

  public DriverSettings(Config config) {
    this.config = config;
  }

  public DseCluster newCluster() {
    DseCluster.Builder builder = DseCluster.builder();
    config
        .getStringList("contact-points")
        .forEach(
            s -> {
              String[] tokens = s.split(":");
              InetSocketAddress address =
                  InetSocketAddress.createUnresolved(tokens[0], Integer.parseInt(tokens[1]));
              builder.addContactPointsWithPorts(address);
            });

    ProtocolVersion protocolVersion = config.getEnum(ProtocolVersion.class, "protocol.version");
    Preconditions.checkArgument(
        protocolVersion.compareTo(ProtocolVersion.V3) >= 0,
        "This loader does not support protocols version lower than 3");
    builder
        .withProtocolVersion(protocolVersion)
        .withCompression(config.getEnum(ProtocolOptions.Compression.class, "protocol.compression"))
        .withPoolingOptions(
            new PoolingOptions()
                .setCoreConnectionsPerHost(
                    HostDistance.LOCAL, config.getInt("pooling.local.connections"))
                .setMaxConnectionsPerHost(
                    HostDistance.LOCAL, config.getInt("pooling.local.connections"))
                .setCoreConnectionsPerHost(
                    HostDistance.REMOTE, config.getInt("pooling.remote.connections"))
                .setMaxConnectionsPerHost(
                    HostDistance.REMOTE, config.getInt("pooling.remote.connections"))
                .setMaxRequestsPerConnection(
                    HostDistance.LOCAL, config.getInt("pooling.local.requests"))
                .setMaxRequestsPerConnection(
                    HostDistance.REMOTE, config.getInt("pooling.remote.requests"))
                .setHeartbeatIntervalSeconds(
                    (int) config.getDuration("pooling.heartbeat").getSeconds()))
        .withQueryOptions(
            new QueryOptions()
                .setConsistencyLevel(config.getEnum(ConsistencyLevel.class, "query.consistency"))
                .setSerialConsistencyLevel(
                    config.getEnum(ConsistencyLevel.class, "query.serial-consistency"))
                .setFetchSize(config.getInt("query.fetch-size"))
                .setDefaultIdempotence(config.getBoolean("query.idempotence")))
        .withSocketOptions(
            new SocketOptions()
                .setReadTimeoutMillis((int) config.getDuration("socket.read-timeout").toMillis()))
        .withTimestampGenerator(newInstance(config.getString("timestamp-generator")))
        .withAddressTranslator(newInstance(config.getString("address-translator")));

    builder.withLoadBalancingPolicy(newInstance(config.getString("policy.lbp")));
    String retry = config.getString("policy.retry");
    if (retry.equals(DEFAULT_RETRY_POLICY_FQDN))
      builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
    else builder.withRetryPolicy(newInstance(retry));
    String specexec = config.getString("policy.specexec");
    if (specexec.equals(NO_SPECULATIVE_EXECUTION_POLICY_FQDN))
      builder.withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE);
    else builder.withSpeculativeExecutionPolicy(newInstance(specexec));
    // TODO configure policies

    if (config.hasPath("auth.provider")) {
      builder.withAuthProvider(newInstance(config.getString("auth.provider")));
      // TODO configure provider
    }
    if (config.hasPath("ssl.engine-factory")) {
      // TODO configure
      builder.withSSL();
    }

    return builder.build();
  }
}
