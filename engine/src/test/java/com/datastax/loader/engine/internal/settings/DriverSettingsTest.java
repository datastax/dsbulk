/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.IdentityTranslator;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.ProtocolOptions.Compression.LZ4;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;

/** */
public class DriverSettingsTest {

  @Test(expected = ConfigException.Missing.class)
  public void should_not_create_mapper_when_contact_points_not_provided() throws Exception {
    Config config = ConfigFactory.load().getConfig("datastax-loader.batch");
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.newCluster();
  }

  @Test
  public void should_create_mapper_when_contact_points_provided() throws Exception {
    Config config =
        ConfigFactory.parseString("contact-points = [ \"1.2.3.4:9042\", \"2.3.4.5:9042\" ]")
            .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver"));
    DriverSettings driverSettings = new DriverSettings(config);
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    Cluster delegate = (Cluster) Whitebox.getInternalState(cluster, "delegate");
    Object manager = Whitebox.getInternalState(delegate, "manager");
    @SuppressWarnings("unchecked")
    List<InetSocketAddress> contactPoints =
        (List<InetSocketAddress>) Whitebox.getInternalState(manager, "contactPoints");
    assertThat(contactPoints)
        .containsExactly(
            new InetSocketAddress("1.2.3.4", 9042), new InetSocketAddress("2.3.4.5", 9042));
    DseConfiguration configuration = cluster.getConfiguration();
    assertThat(
            Whitebox.getInternalState(configuration.getProtocolOptions(), "initialProtocolVersion"))
        .isEqualTo(V4);
    assertThat(configuration.getProtocolOptions().getCompression()).isEqualTo(LZ4);
    QueryOptions queryOptions = configuration.getQueryOptions();
    assertThat(queryOptions.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
    assertThat(queryOptions.getSerialConsistencyLevel()).isEqualTo(LOCAL_SERIAL);
    assertThat(queryOptions.getFetchSize()).isEqualTo(5000);
    assertThat(queryOptions.getDefaultIdempotence()).isTrue();
    PoolingOptions poolingOptions = configuration.getPoolingOptions();
    assertThat(poolingOptions.getCoreConnectionsPerHost(LOCAL)).isEqualTo(1);
    assertThat(poolingOptions.getMaxConnectionsPerHost(LOCAL)).isEqualTo(1);
    assertThat(poolingOptions.getCoreConnectionsPerHost(REMOTE)).isEqualTo(1);
    assertThat(poolingOptions.getMaxConnectionsPerHost(REMOTE)).isEqualTo(1);
    assertThat(poolingOptions.getMaxRequestsPerConnection(LOCAL)).isEqualTo(32768);
    assertThat(poolingOptions.getMaxRequestsPerConnection(REMOTE)).isEqualTo(1024);
    assertThat(poolingOptions.getHeartbeatIntervalSeconds()).isEqualTo(30);
    SocketOptions socketOptions = configuration.getSocketOptions();
    assertThat(socketOptions.getReadTimeoutMillis()).isEqualTo(12000);
    Policies policies = configuration.getPolicies();
    assertThat(policies.getTimestampGenerator())
        .isInstanceOf(AtomicMonotonicTimestampGenerator.class);
    assertThat(policies.getAddressTranslator()).isInstanceOf(IdentityTranslator.class);
    assertThat(policies.getLoadBalancingPolicy()).isInstanceOf(RoundRobinPolicy.class);
    assertThat(policies.getRetryPolicy()).isInstanceOf(RetryPolicy.class);
    assertThat(policies.getSpeculativeExecutionPolicy())
        .isInstanceOf(NoSpeculativeExecutionPolicy.class);
  }
}
