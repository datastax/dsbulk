/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.ProtocolOptions.Compression.LZ4;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.IdentityTranslator;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseConfiguration;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import javax.security.auth.login.Configuration;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class DriverSettingsTest {

  @Test(expected = ConfigException.Missing.class)
  public void should_not_create_mapper_when_contact_points_not_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            new DefaultLoaderConfig(ConfigFactory.load().getConfig("datastax-loader.batch")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    driverSettings.newCluster();
  }

  @Test
  public void should_create_mapper_when_contact_points_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            new DefaultLoaderConfig(
                ConfigFactory.parseString("contactPoints = [ \"1.2.3.4:9042\", \"2.3.4.5:9042\" ]")
                    .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver"))));
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
            new InetSocketAddress("1.2.3.4", 9042), new InetSocketAddress("2.3.4.5", 9042));
    DseConfiguration configuration = cluster.getConfiguration();
    assertThat(
            Whitebox.getInternalState(configuration.getProtocolOptions(), "initialProtocolVersion"))
        .isNull();
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
    assertThat(configuration.getProtocolOptions().getSSLOptions()).isNull();
    assertThat(configuration.getProtocolOptions().getAuthProvider()).isSameAs(AuthProvider.NONE);
  }

  @Test
  public void should_configure_authentication_with_PlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = PlainTextAuthProvider, username = alice, password = s3cr3t }")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
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
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
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
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
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
    boolean isWin = System.getProperty("os.name").toLowerCase().contains("win");
    assertThat(Whitebox.getInternalState(loginConfiguration, "keyTab"))
        .isEqualTo(isWin ? "C:\\path\\to\\my\\keyTab" : "/path/to/my/keyTab");
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
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
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
                            + "   url = \"%s\","
                            + "   password = cassandra1sfun "
                            + "}, "
                            + "truststore { "
                            + "   url = \"%s\","
                            + "   password = cassandra1sfun "
                            + "}"
                            + "}",
                        keystore, truststore))
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
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
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.driver")));
    DriverSettings driverSettings = new DriverSettings(config, "test");
    DseCluster cluster = driverSettings.newCluster();
    assertThat(cluster).isNotNull();
    DseConfiguration configuration = cluster.getConfiguration();
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    assertThat(sslOptions).isInstanceOf(RemoteEndpointAwareNettySSLOptions.class);
    SslContext sslContext = (SslContext) Whitebox.getInternalState(sslOptions, "context");
    // these are the OpenSSL equivalents to JSSE cipher names
    assertThat(sslContext.cipherSuites()).containsExactly("AES128-SHA", "AES256-SHA");
  }
}
