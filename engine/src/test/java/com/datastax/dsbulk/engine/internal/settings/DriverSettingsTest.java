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
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
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
          DriverSettings driverSettings = new DriverSettings(config);
          driverSettings.init();
          driverSettings.newCluster();
        });
  }

  @Test
  void should_create_mapper_when_hosts_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("port = 9876, hosts = [1.2.3.4, 2.3.4.5, 9.8.7.6]")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
            new InetSocketAddress("1.2.3.4", 9876),
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
    DriverSettings driverSettings = new DriverSettings(config);
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
    DriverSettings driverSettings = new DriverSettings(config);
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
  void should_infer_authentication_with_DsePlainTextAuthProvider() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { username = alice, password = s3cr3t, authorizationId = bob }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
    DriverSettings driverSettings = new DriverSettings(config);
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
    DriverSettings driverSettings = new DriverSettings(config);
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
                        quoteJson(keystore), quoteJson(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
                        quoteJson(keyCertChain), quoteJson(privateKey), quoteJson(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
                        + "      localDc=myLocalDcName,"
                        + "      allowRemoteDCsForLocalConsistencyLevel=true,"
                        + "      usedHostsPerRemoteDc=2,"
                        + "    }, "
                        + "    tokenAware {"
                        + "      childPolicy = dcAwareRoundRobin,"
                        + "      replicaOrdering = NEUTRAL,"
                        + "    }, "
                        + "    whiteList {"
                        + "      childPolicy = tokenAware,"
                        + "      hosts = [127.0.0.4, 127.0.0.1],"
                        + "    }, "
                        + "  }"
                        + "}")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
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
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.3", 9123))).isFalse();
    assertThat(predicate.apply(makeHostWithAddress("127.0.0.4", 9123))).isTrue();

    // The whitelist policy chains to a token-aware policy.
    assertThat(whiteListPolicy.getChildPolicy()).isInstanceOf(TokenAwarePolicy.class);
    TokenAwarePolicy tokenAwarePolicy = (TokenAwarePolicy) whiteListPolicy.getChildPolicy();
    assertThat(getInternalState(tokenAwarePolicy, "replicaOrdering"))
        .isEqualTo(TokenAwarePolicy.ReplicaOrdering.NEUTRAL);

    // ...which chains to a DCAwareRoundRobinPolicy
    assertThat(tokenAwarePolicy.getChildPolicy()).isInstanceOf(DCAwareRoundRobinPolicy.class);
    DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy =
        (DCAwareRoundRobinPolicy) tokenAwarePolicy.getChildPolicy();
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "localDc")).isEqualTo("myLocalDcName");
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "dontHopForLocalCL")).isEqualTo(false);
    assertThat(getInternalState(dcAwareRoundRobinPolicy, "usedHostsPerRemoteDc")).isEqualTo(2);
  }

  @Test
  void should_error_on_empty_hosts() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("hosts = []")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "driver.hosts is mandatory. Please set driver.hosts and try again. "
                + "See settings.md or help for more information");
  }

  @Test
  void should_error_bad_parse_driver_option() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("socket.readTimeout=\"I am not a duration\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value at 'socket.readTimeout'");
  }

  @Test
  void should_error_invalid_auth_provider() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("auth.provider = InvalidAuthProvider")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("InvalidAuthProvider is not a valid auth provider");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_username() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("auth.provider=PlainTextAuthProvider, auth.username = \"\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("must be provided with both auth.username and auth.password");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_password() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "auth.provider=DsePlainTextAuthProvider, auth.password = \"\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("must be provided with both auth.username and auth.password");
  }

  @Test
  void should_error_unknown_lbp() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("policy.lbp.name = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value at 'policy.lbp.name'");
  }

  @Test
  void should_error_lbp_bad_child_policy() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("policy.lbp.name = dse, policy.lbp.dse.childPolicy = junk")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value at 'dse.childPolicy'");
  }

  @Test
  void should_error_lbp_chaining_loop_self() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("policy.lbp.name = dse, policy.lbp.dse.childPolicy = dse")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Load balancing policy chaining loop detected: dse,dse");
  }

  @Test
  void should_error_lbp_chaining_loop() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "policy.lbp.name = dse, "
                        + "policy.lbp.dse.childPolicy = whiteList, "
                        + "policy.lbp.whiteList.childPolicy = tokenAware, "
                        + "policy.lbp.tokenAware.childPolicy = whiteList")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Load balancing policy chaining loop detected: dse,whiteList,tokenAware,whiteList");
  }

  @Test
  void should_error_nonexistent_keytab() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "auth.provider = DseGSSAPIAuthProvider, auth.keyTab = noexist.keytab")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*Keytab file .*noexist.keytab does not exist.*");
  }

  @Test
  void should_error_keytab_is_a_dir() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("auth.provider = DseGSSAPIAuthProvider, auth.keyTab = \".\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*Keytab file .* is not a file.*");
  }

  @Test
  void should_error_keytab_has_no_keys() throws Exception {
    Path keytabPath = Files.createTempFile("my", ".keytab");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "auth.provider = DseGSSAPIAuthProvider, auth.keyTab = "
                          + quoteJson(keytabPath))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*Could not find any principals in.*");
    } finally {
      Files.delete(keytabPath);
    }
  }

  @Test
  void should_error_DseGSSAPIAuthProvider_and_no_sasl_protocol() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "auth.provider = DseGSSAPIAuthProvider, auth.saslService = null")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "DseGSSAPIAuthProvider must be provided with auth.saslService. "
                + "auth.principal, auth.keyTab, and auth.authorizationId are optional.");
  }

  @Test
  void should_error_keystore_without_password() throws IOException {
    Path keystore = Files.createTempFile("my", "keystore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = JDK, ssl.keystore.path = " + quoteJson(keystore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "ssl.keystore.path, ssl.keystore.password and ssl.truststore.algorithm must be provided together");
    } finally {
      Files.delete(keystore);
    }
  }

  @Test
  void should_error_password_without_keystore() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("ssl.provider = JDK, ssl.keystore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "ssl.keystore.path, ssl.keystore.password and ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_openssl_keycertchain_without_key() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL, ssl.openssl.keyCertChain = " + quoteJson(chain))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "ssl.openssl.keyCertChain and ssl.openssl.privateKey must be provided together");
    } finally {
      Files.delete(chain);
    }
  }

  @Test
  void should_error_key_without_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL, ssl.openssl.privateKey = " + quoteJson(key))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "ssl.openssl.keyCertChain and ssl.openssl.privateKey must be provided together");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_error_truststore_without_password() throws IOException {
    Path truststore = Files.createTempFile("my", "truststore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = JDK, ssl.truststore.path = " + quoteJson(truststore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "ssl.truststore.path, ssl.truststore.password and ssl.truststore.algorithm must be provided");
    } finally {
      Files.delete(truststore);
    }
  }

  @Test
  void should_error_password_without_truststore() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("ssl.provider = JDK, ssl.truststore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "ssl.truststore.path, ssl.truststore.password and ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_nonexistent_truststore() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "ssl.provider = JDK,"
                        + "ssl.truststore.path = noexist.truststore,"
                        + "ssl.truststore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL truststore file .*noexist.truststore does not exist.*");
  }

  @Test
  void should_error_truststore_is_a_dir() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "ssl.provider = JDK,"
                        + "ssl.truststore.path = \".\","
                        + "ssl.truststore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL truststore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_truststore() throws IOException {
    Path truststore = Files.createTempFile("my", ".truststore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.truststore.password = mypass, ssl.truststore.path = "
                          + quoteJson(truststore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      settings.init();
    } finally {
      Files.delete(truststore);
    }
  }

  @Test
  void should_error_nonexistent_keystore() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "ssl.provider = JDK, "
                        + "ssl.keystore.path = noexist.keystore, "
                        + "ssl.keystore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL keystore file .*noexist.keystore does not exist.*");
  }

  @Test
  void should_error_keystore_is_a_dir() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "ssl.provider = JDK, "
                        + "ssl.keystore.path = \".\", "
                        + "ssl.keystore.password = mypass")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL keystore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_keystore() throws IOException {
    Path keystore = Files.createTempFile("my", ".keystore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.keystore.password = mypass, ssl.keystore.path = " + quoteJson(keystore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      settings.init();
    } finally {
      Files.delete(keystore);
    }
  }

  @Test
  void should_error_nonexistent_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL, "
                          + "ssl.openssl.keyCertChain = noexist.chain, "
                          + "ssl.openssl.privateKey = "
                          + quoteJson(key))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(
              ".*OpenSSL key certificate chain file .*noexist.chain does not exist.*");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_error_openssl_keycertchain_is_a_dir() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL, "
                          + "ssl.openssl.keyCertChain = \".\", "
                          + "ssl.openssl.privateKey = "
                          + quoteJson(key))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL key certificate chain file .* is not a file.*");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_accept_existing_openssl_keycertchain_and_key() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    Path chain = Files.createTempFile("my", ".chain");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL,"
                          + "ssl.openssl.keyCertChain = "
                          + quoteJson(chain)
                          + ", ssl.openssl.privateKey = "
                          + quoteJson(key))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      settings.init();
    } finally {
      Files.delete(key);
      Files.delete(chain);
    }
  }

  @Test
  void should_error_nonexistent_openssl_key() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL,"
                          + "ssl.openssl.privateKey = noexist.key,"
                          + "ssl.openssl.keyCertChain = "
                          + quoteJson(chain))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL private key file .*noexist.key does not exist.*");
    } finally {
      Files.delete(chain);
    }
  }

  @Test
  void should_error_openssl_key_is_a_dir() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.provider = OpenSSL,"
                          + "ssl.openssl.privateKey = \".\","
                          + "ssl.openssl.keyCertChain = "
                          + quoteJson(chain))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(settings::init)
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL private key file .* is not a file.*");
    } finally {
      Files.delete(chain);
    }
  }

  @Test
  void should_throw_exception_when_port_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("port = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for driver.port: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_connections_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.local.connections = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.pooling.local.connections: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_connections_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.remote.connections = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.pooling.remote.connections: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_requests_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.local.requests = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.pooling.local.requests: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_requests_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.remote.requests = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.pooling.remote.requests: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_fetch_size_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.fetchSize = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for driver.query.fetchSize: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_retries_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("policy.maxRetries = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for driver.policy.maxRetries: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_idempotence_not_a_boolean() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.idempotence = NotABoolean")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for driver.query.idempotence: Expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_timestamp_generator_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("timestampGenerator = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.timestampGenerator: Expecting FQCN or short class name, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_address_translator_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("addressTranslator = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.addressTranslator: Expecting FQCN or short class name, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_compression_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("protocol.compression = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'protocol.compression': Expecting one of NONE, SNAPPY, LZ4, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_consistency_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.consistency = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'query.consistency': Expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_serial_consistency_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.serialConsistency = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'query.serialConsistency': Expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_read_timeout_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("socket.readTimeout = NotADuration")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'socket.readTimeout': No number in duration value 'NotADuration'");
  }

  @Test
  void should_throw_exception_when_heartbeat_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.heartbeat = NotADuration")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'pooling.heartbeat': No number in duration value 'NotADuration'");
  }

  private static Host makeHostWithAddress(String host, int port) {
    Host h = Mockito.mock(Host.class);
    Mockito.when(h.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
    return h;
  }
}
