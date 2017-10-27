/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.ProtocolOptions.Compression.NONE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.IdentityTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseConfiguration;
import com.datastax.driver.dse.DseLoadBalancingPolicy;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.engine.internal.policies.MultipleRetryPolicy;
import com.google.common.base.Predicate;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import javax.security.auth.login.Configuration;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class DriverSettingsTest {

  @Test(expected = ConfigException.Missing.class)
  public void should_not_create_mapper_when_contact_points_not_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.batch")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.newCluster();
  }

  @Test
  public void should_create_mapper_when_hosts_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("port = 9876, hosts = \"1.2.3.4:9042, 2.3.4.5,9.8.7.6\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    Cluster delegate = (Cluster) Whitebox.getInternalState(cluster, "delegate");
    Object manager = Whitebox.getInternalState(delegate, "manager");
    @SuppressWarnings("unchecked")
    List<InetSocketAddress> contactPoints =
        (List<InetSocketAddress>) Whitebox.getInternalState(manager, "contactPoints");
    assertThat(contactPoints)
        .containsExactly(
            new InetSocketAddress("1.2.3.4", 9042),
            new InetSocketAddress("2.3.4.5", 9876),
            new InetSocketAddress("9.8.7.6", 9876));
    DseConfiguration configuration = cluster.getConfiguration();
    assertThat(
            Whitebox.getInternalState(configuration.getProtocolOptions(), "initialProtocolVersion"))
        .isNull();
    assertThat(configuration.getProtocolOptions().getCompression()).isEqualTo(NONE);
    QueryOptions queryOptions = configuration.getQueryOptions();
    assertThat(queryOptions.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
    assertThat(queryOptions.getSerialConsistencyLevel()).isEqualTo(LOCAL_SERIAL);
    assertThat(queryOptions.getFetchSize()).isEqualTo(5000);
    assertThat(queryOptions.getDefaultIdempotence()).isTrue();
    PoolingOptions poolingOptions = configuration.getPoolingOptions();
    assertThat(poolingOptions.getCoreConnectionsPerHost(LOCAL)).isEqualTo(4);
    assertThat(poolingOptions.getMaxConnectionsPerHost(LOCAL)).isEqualTo(4);
    assertThat(poolingOptions.getCoreConnectionsPerHost(REMOTE)).isEqualTo(1);
    assertThat(poolingOptions.getMaxConnectionsPerHost(REMOTE)).isEqualTo(1);
    assertThat(poolingOptions.getMaxRequestsPerConnection(LOCAL)).isEqualTo(32768);
    assertThat(poolingOptions.getMaxRequestsPerConnection(REMOTE)).isEqualTo(1024);
    assertThat(poolingOptions.getHeartbeatIntervalSeconds()).isEqualTo(30);
    SocketOptions socketOptions = configuration.getSocketOptions();
    assertThat(socketOptions.getReadTimeoutMillis()).isEqualTo(60000);
    Policies policies = configuration.getPolicies();
    assertThat(policies.getTimestampGenerator())
        .isInstanceOf(AtomicMonotonicTimestampGenerator.class);
    assertThat(policies.getAddressTranslator()).isInstanceOf(IdentityTranslator.class);
    assertThat(policies.getLoadBalancingPolicy()).isInstanceOf(DseLoadBalancingPolicy.class);
    assertThat(policies.getRetryPolicy()).isInstanceOf(MultipleRetryPolicy.class);
    assertThat(Whitebox.getInternalState(policies.getRetryPolicy(), "maxRetryCount")).isEqualTo(10);
    assertThat(policies.getSpeculativeExecutionPolicy())
        .isInstanceOf(NoSpeculativeExecutionPolicy.class);
    assertThat(configuration.getProtocolOptions().getSSLOptions()).isNull();
    assertThat(configuration.getProtocolOptions().getAuthProvider()).isSameAs(AuthProvider.NONE);
  }

  @Test
  public void should_configure_authentication_with_PlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = PlainTextAuthProvider, username = alice, password = s3cr3t }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(PlainTextAuthProvider.class);
    assertThat(Whitebox.getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(Whitebox.getInternalState(provider, "password")).isEqualTo("s3cr3t");
  }

  @Test
  public void should_configure_authentication_with_DsePlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = DsePlainTextAuthProvider, username = alice, password = s3cr3t, authorizationId = bob }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DsePlainTextAuthProvider.class);
    assertThat(Whitebox.getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(Whitebox.getInternalState(provider, "password")).isEqualTo("s3cr3t");
    assertThat(Whitebox.getInternalState(provider, "authorizationId")).isEqualTo("bob");
  }

  @Test
  public void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab()
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { "
                        + "provider = DseGSSAPIAuthProvider, "
                        + "principal = \"alice@DATASTAX.COM\", "
                        + "keyTab = \"file:///path/to/my/keyTab\", "
                        + "authorizationId = \"bob@DATASTAX.COM\","
                        + "saslProtocol = foo }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(Whitebox.getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(Whitebox.getInternalState(provider, "authorizationId"))
        .isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) Whitebox.getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.KeyTabConfiguration.class);
    assertThat(Whitebox.getInternalState(loginConfiguration, "principal"))
        .isEqualTo("alice@DATASTAX.COM");
    assertThat(Whitebox.getInternalState(loginConfiguration, "keyTab"))
        .isEqualTo(PlatformUtils.isWindows() ? "C:\\path\\to\\my\\keyTab" : "/path/to/my/keyTab");
  }

  @Test
  public void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache()
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { "
                        + "provider = DseGSSAPIAuthProvider, "
                        + "principal = \"alice@DATASTAX.COM\", "
                        + "authorizationId = \"bob@DATASTAX.COM\","
                        + "saslProtocol = foo }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(Whitebox.getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(Whitebox.getInternalState(provider, "authorizationId"))
        .isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) Whitebox.getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.TicketCacheConfiguration.class);
    assertThat(Whitebox.getInternalState(loginConfiguration, "principal"))
        .isEqualTo("alice@DATASTAX.COM");
  }

  @Test
  public void should_configure_encryption_with_SSLContext() throws Exception {
    URL keystore = getClass().getResource("/client.keystore");
    URL truststore = getClass().getResource("/client.truststore");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " ssl { "
                            + "provider = JDK, "
                            + "cipherSuites = [ \"TLS_RSA_WITH_AES_128_CBC_SHA\", \"TLS_RSA_WITH_AES_256_CBC_SHA\" ], "
                            + "keystore { "
                            + "   path = \"%s\","
                            + "   password = cassandra1sfun "
                            + "}, "
                            + "truststore { "
                            + "   path = \"%s\","
                            + "   password = cassandra1sfun "
                            + "}"
                            + "}",
                        keystore, truststore))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareJdkSSLOptions.class);
    assertThat(Whitebox.getInternalState(sslOptions, "cipherSuites"))
        .isEqualTo(new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"});
  }

  @Test
  public void should_configure_encryption_with_OpenSSL() throws Exception {
    URL keyCertChain = getClass().getResource("/client.crt");
    URL privateKey = getClass().getResource("/client.key");
    URL truststore = getClass().getResource("/client.truststore");
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " ssl { "
                            + "provider = OpenSSL, "
                            + "cipherSuites = [ \"TLS_RSA_WITH_AES_128_CBC_SHA\", \"TLS_RSA_WITH_AES_256_CBC_SHA\" ], "
                            + "openssl { "
                            + "   keyCertChain = \"%s\","
                            + "   privateKey = \"%s\""
                            + "}, "
                            + "truststore { "
                            + "   url = \"%s\","
                            + "   password = cassandra1sfun "
                            + "}"
                            + "}",
                        keyCertChain, privateKey, truststore))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareNettySSLOptions.class);
    SslContext sslContext = (SslContext) Whitebox.getInternalState(sslOptions, "context");
    assertThat(sslContext.cipherSuites())
        .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
  }

  @Test
  public void should_configure_lbp() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " port = 9123, "
                        + "policy { "
                        + "  lbp { "
                        + "    name=dse, "
                        + "    dse {"
                        + "      childPolicy=whiteList,"
                        + "    }, "
                        + "    dcAwareRoundRobin {"
                        + "      localDc=127.0.0.2,"
                        + "      allowRemoteDCsForLocalConsistencyLevel=true,"
                        + "      usedHostsPerRemoteDc=2,"
                        + "    }, "
                        + "    tokenAware {"
                        + "      childPolicy = dcAwareRoundRobin,"
                        + "      shuffleReplicas = false,"
                        + "    }, "
                        + "    whiteList {"
                        + "      childPolicy = tokenAware,"
                        + "      hosts = \"127.0.0.4:9000, 127.0.0.1\","
                        + "    }, "
                        + "  }"
                        + "}")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();

    // The main lbp is a DseLoadBalancingPolicy
    LoadBalancingPolicy lbp = configuration.getPolicies().getLoadBalancingPolicy();
    assertThat(lbp).isInstanceOf(DseLoadBalancingPolicy.class);
    DseLoadBalancingPolicy dseLbp = (DseLoadBalancingPolicy) lbp;

    // ...which chains to a WhiteListPolicy
    assertThat(dseLbp.getChildPolicy()).isInstanceOf(WhiteListPolicy.class);
    WhiteListPolicy whiteListPolicy = (WhiteListPolicy) dseLbp.getChildPolicy();

    // ... whose host-list is not reachable. But we can invoke the predicate to
    // verify that our two hosts are in the white list.
    @SuppressWarnings({"unchecked", "Guava"})
    Predicate<Host> predicate =
        (Predicate<Host>) Whitebox.getInternalState(whiteListPolicy, "predicate");
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.1", 9123))).isTrue();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9123))).isFalse();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9000))).isTrue();

    // The whitelist policy chains to a token-aware policy.
    assertThat(whiteListPolicy.getChildPolicy()).isInstanceOf(TokenAwarePolicy.class);
    TokenAwarePolicy tokenAwarePolicy = (TokenAwarePolicy) whiteListPolicy.getChildPolicy();
    assertThat(Whitebox.getInternalState(tokenAwarePolicy, "shuffleReplicas")).isEqualTo(false);

    // ...which chains to a DCAwareRoundRobinPolicy
    assertThat(tokenAwarePolicy.getChildPolicy()).isInstanceOf(DCAwareRoundRobinPolicy.class);
    DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy =
        (DCAwareRoundRobinPolicy) tokenAwarePolicy.getChildPolicy();
    assertThat(Whitebox.getInternalState(dcAwareRoundRobinPolicy, "localDc"))
        .isEqualTo("127.0.0.2");
    assertThat(Whitebox.getInternalState(dcAwareRoundRobinPolicy, "dontHopForLocalCL"))
        .isEqualTo(false);
    assertThat(Whitebox.getInternalState(dcAwareRoundRobinPolicy, "usedHostsPerRemoteDc"))
        .isEqualTo(2);
  }

  private Host makeHostWithAddress(String host, int port) {
    Host h = Mockito.mock(Host.class);
    Mockito.when(h.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
    return h;
  }
}
