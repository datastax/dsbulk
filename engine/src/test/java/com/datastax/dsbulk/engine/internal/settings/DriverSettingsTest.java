/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.ProtocolOptions.Compression.NONE;
import static com.datastax.dsbulk.commons.tests.utils.ReflectionUtils.getInternalState;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import com.datastax.dsbulk.engine.internal.policies.MultipleRetryPolicy;
import com.google.common.base.Predicate;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.security.auth.login.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    Cluster delegate = (Cluster) getInternalState(cluster, "delegate");
    Object manager = getInternalState(delegate, "manager");
    @SuppressWarnings("unchecked")
    List<InetSocketAddress> contactPoints =
        (List<InetSocketAddress>) getInternalState(manager, "contactPoints");
    assertThat(contactPoints)
        .containsExactly(
            new InetSocketAddress("1.2.3.4", 9042),
            new InetSocketAddress("2.3.4.5", 9876),
            new InetSocketAddress("9.8.7.6", 9876));
    DseConfiguration configuration = cluster.getConfiguration();
    assertThat(getInternalState(configuration.getProtocolOptions(), "initialProtocolVersion"))
        .isNull();
    assertThat(configuration.getProtocolOptions().getCompression()).isEqualTo(NONE);
    QueryOptions queryOptions = configuration.getQueryOptions();
    assertThat(queryOptions.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
    assertThat(queryOptions.getSerialConsistencyLevel()).isEqualTo(LOCAL_SERIAL);
    assertThat(queryOptions.getFetchSize()).isEqualTo(5000);
    assertThat(queryOptions.getDefaultIdempotence()).isTrue();
    PoolingOptions poolingOptions = configuration.getPoolingOptions();
    assertThat(poolingOptions.getCoreConnectionsPerHost(LOCAL)).isEqualTo(8);
    assertThat(poolingOptions.getMaxConnectionsPerHost(LOCAL)).isEqualTo(8);
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
    assertThat(getInternalState(policies.getRetryPolicy(), "maxRetryCount")).isEqualTo(10);
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
    assertThat(getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(getInternalState(provider, "password")).isEqualTo("s3cr3t");
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
    assertThat(getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(getInternalState(provider, "password")).isEqualTo("s3cr3t");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob");
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab()
      throws URISyntaxException {
    Path keyTab = Paths.get(getClass().getResource("/cassandra.keytab").toURI());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " auth { "
                            + "provider = DseGSSAPIAuthProvider , "
                            + "keyTab = \"%s\", "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        escapeUserInput(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal"))
        .isEqualTo("cassandra@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab_and_principal()
      throws URISyntaxException {
    Path keyTab = Paths.get(getClass().getResource("/cassandra.keytab").toURI());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " auth { "
                            + "provider = DseGSSAPIAuthProvider , "
                            + "principal = \"alice@DATASTAX.COM\", "
                            + "keyTab = \"%s\", "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        escapeUserInput(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_empty_keytab_and_principal()
      throws URISyntaxException {
    // Get the path to the "empty keytab". We emulate that by choosing a file that isn't
    // a keytab at all. Since we're specifying the principal, it shouldn't matter that the
    // keytab is empty (e.g. we shouldn't even be checking).
    Path keyTab = Paths.get(getClass().getResource("/client.key").toURI());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " auth { "
                            + "provider = DseGSSAPIAuthProvider, "
                            + "principal = \"alice@DATASTAX.COM\", "
                            + "keyTab = \"%s\", "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        escapeUserInput(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { "
                        + "provider = DseGSSAPIAuthProvider, "
                        + "authorizationId = \"bob@DATASTAX.COM\","
                        + "saslService = foo }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.TicketCacheConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isNull();
  }

  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache_and_principal() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { "
                        + "provider = DseGSSAPIAuthProvider, "
                        + "principal = \"alice@DATASTAX.COM\", "
                        + "authorizationId = \"bob@DATASTAX.COM\","
                        + "saslService = foo }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    AuthProvider provider = configuration.getProtocolOptions().getAuthProvider();
    assertThat(provider).isInstanceOf(DseGSSAPIAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(DriverSettings.TicketCacheConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
  }

  @Test
  void should_configure_encryption_with_SSLContext() throws URISyntaxException {
    Path keystore = Paths.get(getClass().getResource("/client.keystore").toURI());
    Path truststore = Paths.get(getClass().getResource("/client.truststore").toURI());
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
                        escapeUserInput(keystore), escapeUserInput(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareJdkSSLOptions.class);
    assertThat(getInternalState(sslOptions, "cipherSuites"))
        .isEqualTo(new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"});
  }

  @Test
  void should_configure_encryption_with_OpenSSL() throws URISyntaxException {
    Path keyCertChain = Paths.get(getClass().getResource("/client.crt").toURI());
    Path privateKey = Paths.get(getClass().getResource("/client.key").toURI());
    Path truststore = Paths.get(getClass().getResource("/client.truststore").toURI());
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
                        escapeUserInput(keyCertChain),
                        escapeUserInput(privateKey),
                        escapeUserInput(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.init();
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareNettySSLOptions.class);
    SslContext sslContext = (SslContext) getInternalState(sslOptions, "context");
    assertThat(sslContext.cipherSuites())
        .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
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
    Predicate<Host> predicate = (Predicate<Host>) getInternalState(whiteListPolicy, "predicate");
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.1", 9123))).isTrue();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9123))).isFalse();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9000))).isTrue();

    // The whitelist policy chains to a token-aware policy.
    assertThat(whiteListPolicy.getChildPolicy()).isInstanceOf(TokenAwarePolicy.class);
    TokenAwarePolicy tokenAwarePolicy = (TokenAwarePolicy) whiteListPolicy.getChildPolicy();
    assertThat(getInternalState(tokenAwarePolicy, "shuffleReplicas")).isEqualTo(false);

    // ...which chains to a DCAwareRoundRobinPolicy
    assertThat(tokenAwarePolicy.getChildPolicy()).isInstanceOf(DCAwareRoundRobinPolicy.class);
    DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy =
        (DCAwareRoundRobinPolicy) tokenAwarePolicy.getChildPolicy();
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "localDc")).isEqualTo("127.0.0.2");
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "dontHopForLocalCL")).isEqualTo(false);
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "usedHostsPerRemoteDc")).isEqualTo(2);
  }

  private static Host makeHostWithAddress(String host, int port) {
    Host h = Mockito.mock(Host.class);
    Mockito.when(h.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
    return h;
  }
}
