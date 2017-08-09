/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.loader.engine.internal.ReflectionUtils.newInstance;
import static com.datastax.loader.engine.internal.ReflectionUtils.resolveClass;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.dse.DseCluster;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.net.InetSocketAddress;

/** */
public class DriverSettings {

  private final Config config;
  private final String operationId;

  DriverSettings(Config config, String operationId) {
    this.config = config;
    this.operationId = operationId;
  }

  public DseCluster newCluster() {
    DseCluster.Builder builder = DseCluster.builder().withClusterName(operationId + "-driver");
    config
        .getStringList("contactPoints")
        .forEach(
            s -> {
              String[] tokens = s.split(":");
              builder.addContactPointsWithPorts(
                  new InetSocketAddress(tokens[0], Integer.parseInt(tokens[1])));
            });

    ProtocolVersion protocolVersion;

    if (config.hasPath("protocol.version")) {
      protocolVersion = config.getEnum(ProtocolVersion.class, "protocol.version");
      Preconditions.checkArgument(
          protocolVersion.compareTo(ProtocolVersion.V3) >= 0,
          "This loader does not support protocol versions lower than 3");
      builder.withProtocolVersion(protocolVersion);
    }
    builder
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
                    config.getEnum(ConsistencyLevel.class, "query.serialConsistency"))
                .setFetchSize(config.getInt("query.fetchSize"))
                .setDefaultIdempotence(config.getBoolean("query.idempotence")))
        .withSocketOptions(
            new SocketOptions()
                .setReadTimeoutMillis((int) config.getDuration("socket.readTimeout").toMillis()))
        .withTimestampGenerator(newInstance(config.getString("timestampGenerator")))
        .withAddressTranslator(newInstance(config.getString("addressTranslator")));

    builder.withLoadBalancingPolicy(newInstance(config.getString("policy.lbp")));
    Class<RetryPolicy> retryPolicyClass = resolveClass(config.getString("policy.retry"));
    if (retryPolicyClass.equals(DefaultRetryPolicy.class)) {
      builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
    } else {
      builder.withRetryPolicy(newInstance(retryPolicyClass));
    }
    Class<SpeculativeExecutionPolicy> speculativeExecutionPolicyClass =
        resolveClass(config.getString("policy.specexec"));
    if (speculativeExecutionPolicyClass.equals(NoSpeculativeExecutionPolicy.class)) {
      builder.withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE);
    } else {
      builder.withSpeculativeExecutionPolicy(newInstance(speculativeExecutionPolicyClass));
    }
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
