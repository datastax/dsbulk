/*
 * Copyright DataStax Inc.
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumingThat;

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
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.utils.PlatformUtils;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.engine.internal.policies.MultipleRetryPolicy;
import com.google.common.base.Predicate;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import javax.security.auth.login.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** */
class DriverSettingsTest {

  @Test
  void should_not_create_mapper_when_contact_points_not_provided() {
    assertThrows(
        BulkConfigurationException.class,
        () -> {
          LoaderConfig config =
              new DefaultLoaderConfig(
                  new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.batch")));
          DriverSettings driverSettings = new DriverSettings(config, "test");
          driverSettings.init();
          driverSettings.newCluster();
        });
  }

  @Test
  void should_create_mapper_when_hosts_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("port = 9876, hosts = \"1.2.3.4:9042, 2.3.4.5,9.8.7.6\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    Cluster delegate = (Cluster) ReflectionUtils.getInternalState(cluster, "delegate");
    Object manager = ReflectionUtils.getInternalState(delegate, "manager");
    @SuppressWarnings("unchecked")
    List<InetSocketAddress> contactPoints =
        (List<InetSocketAddress>) ReflectionUtils.getInternalState(manager, "contactPoints");
    assertThat(contactPoints)
        .containsExactly(
            new InetSocketAddress("1.2.3.4", 9042),
            new InetSocketAddress("2.3.4.5", 9876),
            new InetSocketAddress("9.8.7.6", 9876));
    DseConfiguration configuration = cluster.getConfiguration();
    assertThat(
            ReflectionUtils.getInternalState(
                configuration.getProtocolOptions(), "initialProtocolVersion"))
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
    assertThat(ReflectionUtils.getInternalState(policies.getRetryPolicy(), "maxRetryCount"))
        .isEqualTo(10);
    assertThat(policies.getSpeculativeExecutionPolicy())
        .isInstanceOf(NoSpeculativeExecutionPolicy.class);
    assertThat(configuration.getProtocolOptions().getSSLOptions()).isNull();
    assertThat(configuration.getProtocolOptions().getAuthProvider()).isSameAs(AuthProvider.NONE);
  }

  @Test
  void should_configure_authentication_with_PlainTextAuthProvider() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = PlainTextAuthProvider, username = alice, password = s3cr3t }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(PlainTextAuthProvider.class);
    assertThat(ReflectionUtils.getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(ReflectionUtils.getInternalState(provider, "password")).isEqualTo("s3cr3t");
  }

  @Test
  void should_configure_authentication_with_DsePlainTextAuthProvider() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = DsePlainTextAuthProvider, username = alice, password = s3cr3t, authorizationId = bob }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DsePlainTextAuthProvider.class);
    assertThat(ReflectionUtils.getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(ReflectionUtils.getInternalState(provider, "password")).isEqualTo("s3cr3t");
    assertThat(ReflectionUtils.getInternalState(provider, "authorizationId")).isEqualTo("bob");
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab() {
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
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(ReflectionUtils.getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(ReflectionUtils.getInternalState(provider, "authorizationId"))
        .isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) ReflectionUtils.getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.KeyTabConfiguration.class);
    assertThat(ReflectionUtils.getInternalState(loginConfiguration, "principal"))
        .isEqualTo("alice@DATASTAX.COM");
    assertThat(ReflectionUtils.getInternalState(loginConfiguration, "keyTab"))
        .isEqualTo("/path/to/my/keyTab");
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache() {
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
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(ReflectionUtils.getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(ReflectionUtils.getInternalState(provider, "authorizationId"))
        .isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) ReflectionUtils.getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.TicketCacheConfiguration.class);
    assertThat(ReflectionUtils.getInternalState(loginConfiguration, "principal"))
        .isEqualTo("alice@DATASTAX.COM");
  }

  @Test
  void should_configure_encryption_with_SSLContext() {
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
                            + "   password = cassandra1sfun"
                            + "}"
                            + "}",
                        keystore, truststore))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareJdkSSLOptions.class);
    assertThat(ReflectionUtils.getInternalState(sslOptions, "cipherSuites"))
        .isEqualTo(new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"});
  }

  @Test
  void should_configure_encryption_with_OpenSSL() {
    assumingThat(
        PlatformUtils.isWindows(),
        () -> {
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
                                  + "   path = \"%s\","
                                  + "   password = cassandra1sfun "
                                  + "}"
                                  + "}",
                              keyCertChain, privateKey, truststore))
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
          DriverSettings driverSettings = new DriverSettings(config, "test");
          driverSettings.init();
          DseCluster cluster = driverSettings.newCluster();
          assertThat(cluster).isNotNull();
          DseConfiguration configuration = cluster.getConfiguration();
          SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
          assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareNettySSLOptions.class);
          SslContext sslContext =
              (SslContext) ReflectionUtils.getInternalState(sslOptions, "context");
          assertThat(sslContext.cipherSuites())
              .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
        });
  }

  @Test
  void should_configure_lbp() {
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
    driverSettings.init();
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
        (Predicate<Host>) ReflectionUtils.getInternalState(whiteListPolicy, "predicate");
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.1", 9123))).isTrue();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9123))).isFalse();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9000))).isTrue();

    // The whitelist policy chains to a token-aware policy.
    assertThat(whiteListPolicy.getChildPolicy()).isInstanceOf(TokenAwarePolicy.class);
    TokenAwarePolicy tokenAwarePolicy = (TokenAwarePolicy) whiteListPolicy.getChildPolicy();
    assertThat(ReflectionUtils.getInternalState(tokenAwarePolicy, "shuffleReplicas"))
        .isEqualTo(false);

    // ...which chains to a DCAwareRoundRobinPolicy
    assertThat(tokenAwarePolicy.getChildPolicy()).isInstanceOf(DCAwareRoundRobinPolicy.class);
    DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy =
        (DCAwareRoundRobinPolicy) tokenAwarePolicy.getChildPolicy();
    assertThat(ReflectionUtils.getInternalState(dcAwareRoundRobinPolicy, "localDc"))
        .isEqualTo("127.0.0.2");
    assertThat(ReflectionUtils.getInternalState(dcAwareRoundRobinPolicy, "dontHopForLocalCL"))
        .isEqualTo(false);
    assertThat(ReflectionUtils.getInternalState(dcAwareRoundRobinPolicy, "usedHostsPerRemoteDc"))
        .isEqualTo(2);
  }

  private Host makeHostWithAddress(String host, int port) {
    Host h = Mockito.mock(Host.class);
    Mockito.when(h.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
    return h;
  }
}
