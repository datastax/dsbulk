/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.utils.ReflectionUtils.getInternalState;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_MAX_REQUESTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;
import static org.slf4j.event.Level.WARN;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.engine.internal.auth.AuthProviderFactory.KeyTabConfiguration;
import com.datastax.dsbulk.engine.internal.auth.AuthProviderFactory.TicketCacheConfiguration;
import com.datastax.dsbulk.engine.internal.policies.lbp.DCInferringDseLoadBalancingPolicy;
import com.datastax.dsbulk.engine.internal.policies.retry.MultipleRetryPolicy;
import com.datastax.dsbulk.engine.internal.ssl.JdkSslEngineFactory;
import com.datastax.dsbulk.engine.internal.ssl.NettySslHandlerFactory;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider;
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider;
import com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import javax.security.auth.login.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(SimulacronExtension.class)
class DriverSettingsTest {

  @Test
  void should_not_create_session_when_contact_points_not_provided() {
    assertThrows(
        AllNodesFailedException.class,
        () -> {
          LoaderConfig config =
              new DefaultLoaderConfig(
                  new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.driver")));
          DriverSettings driverSettings = new DriverSettings(config);
          driverSettings.init(true, new HashMap<>());
          driverSettings.newSession("test");
        });
  }

  private void resetPrimes(BoundCluster simulacron) {
    simulacron.clearPrimes(true);
    SimulacronUtils.primeSystemLocal(simulacron, Collections.emptyMap());
    SimulacronUtils.primeSystemPeers(simulacron);
    SimulacronUtils.primeSystemPeersV2(simulacron);
  }

  @Test
  void should_create_session_when_hosts_provided(BoundCluster simulacron) throws Exception {
    resetPrimes(simulacron);
    InetSocketAddress contactPoint = simulacron.node(0).inetSocketAddress();
    int port = contactPoint.getPort();
    String hostAddress = contactPoint.getAddress().getHostAddress();
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(String.format("port = %d, hosts = [%s]", port, hostAddress))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();

    DriverContext context = session.getContext();
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    List<String> contactPoints = profile.getStringList(DefaultDriverOption.CONTACT_POINTS);
    assertThat(contactPoints).containsExactly(String.format("%s:%d", hostAddress, port));

    assertThat(profile.isDefined(DefaultDriverOption.PROTOCOL_COMPRESSION)).isFalse();
    assertThat(profile.getString(REQUEST_CONSISTENCY)).isEqualTo("LOCAL_ONE");
    assertThat(profile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo("LOCAL_SERIAL");
    assertThat(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(5000);
    assertThat(profile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)).isTrue();
    assertThat(profile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).isEqualTo(8);
    assertThat(profile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).isEqualTo(1);
    assertThat(profile.getInt(CONNECTION_MAX_REQUESTS)).isEqualTo(32768);
    assertThat(profile.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .isEqualTo(Duration.ofSeconds(30));
    assertThat(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(60));
    assertThat(profile.getString(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS))
        .isEqualTo(AtomicTimestampGenerator.class.getSimpleName());
    assertThat(profile.getString(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS))
        .isEqualTo(PassThroughAddressTranslator.class.getSimpleName());
    assertThat(profile.getString(LOAD_BALANCING_POLICY_CLASS))
        .isEqualTo(DCInferringDseLoadBalancingPolicy.class.getName());
    assertThat(profile.getString(DefaultDriverOption.RETRY_POLICY_CLASS))
        .isEqualTo(MultipleRetryPolicy.class.getName());
    assertThat(profile.getInt(BulkDriverOption.RETRY_POLICY_MAX_RETRIES)).isEqualTo(10);
    assertThat(profile.getString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS))
        .isEqualTo(NoSpeculativeExecutionPolicy.class.getSimpleName());

    assertThat(context.getAddressTranslator()).isInstanceOf(PassThroughAddressTranslator.class);
    assertThat(context.getTimestampGenerator()).isInstanceOf(AtomicTimestampGenerator.class);
    assertThat(context.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME))
        .isInstanceOf(DCInferringDseLoadBalancingPolicy.class);
    assertThat(context.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME))
        .isInstanceOf(MultipleRetryPolicy.class);
    assertThat(context.getAuthProvider()).isEmpty();
    assertThat(((InternalDriverContext) context).getSslHandlerFactory()).isEmpty();
  }

  @Disabled
  @Test
  void should_configure_authentication_with_PlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = PlainTextAuthProvider, username = alice, password = s3cr3t }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(PlainTextAuthProvider.class);
    assertThat(getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(getInternalState(provider, "password")).isEqualTo("s3cr3t");
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DsePlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { provider = DsePlainTextAuthProvider, username = alice, password = s3cr3t, authorizationId = bob }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DsePlainTextAuthProvider.class);
    assertThat(getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(getInternalState(provider, "password")).isEqualTo("s3cr3t");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob");
  }

  @Disabled
  @Test
  void should_infer_authentication_with_DsePlainTextAuthProvider() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { username = alice, password = s3cr3t, authorizationId = bob }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DsePlainTextAuthProvider.class);
    assertThat(getInternalState(provider, "username")).isEqualTo("alice");
    assertThat(getInternalState(provider, "password")).isEqualTo("s3cr3t");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob");
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab() throws Exception {
    Path keyTab = Paths.get(getClass().getResource("/cassandra.keytab").toURI());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " auth { "
                            + "provider = DseGSSAPIAuthProvider , "
                            + "keyTab = %s, "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DseGssApiAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal"))
        .isEqualTo("cassandra@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_keytab_and_principal()
      throws Exception {
    Path keyTab = Paths.get(getClass().getResource("/cassandra.keytab").toURI());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        " auth { "
                            + "provider = DseGSSAPIAuthProvider , "
                            + "principal = \"alice@DATASTAX.COM\", "
                            + "keyTab = %s, "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DseGssApiAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_empty_keytab_and_principal()
      throws Exception {
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
                            + "keyTab = %s, "
                            + "authorizationId = \"bob@DATASTAX.COM\","
                            + "saslService = foo }",
                        quoteJson(keyTab)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DseGssApiAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(KeyTabConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
    String loginConfigKeyTab = (String) getInternalState(loginConfiguration, "keyTab");
    assertThat(loginConfigKeyTab).isEqualTo(keyTab.toString());
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache()
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " auth { "
                        + "provider = DseGSSAPIAuthProvider, "
                        + "authorizationId = \"bob@DATASTAX.COM\","
                        + "saslService = foo }")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DseGssApiAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(TicketCacheConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isNull();
  }

  @Disabled
  @Test
  void should_configure_authentication_with_DseGSSAPIAuthProvider_and_ticket_cache_and_principal()
      throws Exception {
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
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    DriverContext context = session.getContext();
    assertThat(context.getAuthProvider()).isPresent();
    AuthProvider provider = context.getAuthProvider().get();
    assertThat(provider).isInstanceOf(DseGssApiAuthProvider.class);
    assertThat(getInternalState(provider, "saslProtocol")).isEqualTo("foo");
    assertThat(getInternalState(provider, "authorizationId")).isEqualTo("bob@DATASTAX.COM");
    Configuration loginConfiguration =
        (Configuration) getInternalState(provider, "loginConfiguration");
    assertThat(loginConfiguration).isInstanceOf(TicketCacheConfiguration.class);
    assertThat(getInternalState(loginConfiguration, "principal")).isEqualTo("alice@DATASTAX.COM");
  }

  @Disabled
  @Test
  void should_configure_encryption_with_SSLContext() throws Exception {
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
                            + "   path = %s,"
                            + "   password = cassandra1sfun "
                            + "}, "
                            + "truststore { "
                            + "   path = %s,"
                            + "   password = cassandra1sfun"
                            + "}"
                            + "}",
                        quoteJson(keystore), quoteJson(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    assertThat(context.getSslEngineFactory()).isEmpty();
    assertThat(context.getSslHandlerFactory()).isPresent();
    SslHandlerFactory sslHandlerFactory = context.getSslHandlerFactory().get();
    assertThat(sslHandlerFactory).isInstanceOf(JdkSslEngineFactory.class);
    assertThat(getInternalState(sslHandlerFactory, "cipherSuites"))
        .isEqualTo(new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"});
  }

  @Disabled
  @Test
  void should_configure_encryption_with_OpenSSL() throws Exception {
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
                            + "   keyCertChain = %s,"
                            + "   privateKey = %s"
                            + "}, "
                            + "truststore { "
                            + "   path = %s,"
                            + "   password = cassandra1sfun "
                            + "}"
                            + "}",
                        quoteJson(keyCertChain), quoteJson(privateKey), quoteJson(truststore)))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    assertThat(context.getSslEngineFactory()).isEmpty();
    assertThat(context.getSslHandlerFactory()).isPresent();
    SslHandlerFactory sslHandlerFactory = context.getSslHandlerFactory().get();
    assertThat(sslHandlerFactory).isInstanceOf(NettySslHandlerFactory.class);
    SslContext sslContext = (SslContext) getInternalState(sslHandlerFactory, "context");
    assertThat(sslContext.cipherSuites())
        .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
  }

  @Disabled
  @Test
  void should_configure_lbp() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    " port = 9042, "
                        + "policy { "
                        + "  lbp { "
                        + "    localDc = myLocalDcName,"
                        + "    whiteList  = [127.0.0.4, 127.0.0.1]"
                        + "  }"
                        + "}")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings driverSettings = new DriverSettings(config);
    driverSettings.init(true, new HashMap<>());
    DseSession session = driverSettings.newSession("test");
    assertThat(session).isNotNull();

    DriverContext context = session.getContext();

    // The main lbp is a DseLoadBalancingPolicy
    LoadBalancingPolicy lbp = context.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
    assertThat(lbp).isInstanceOf(DseLoadBalancingPolicy.class);
    DseLoadBalancingPolicy dseLbp = (DseLoadBalancingPolicy) lbp;

    // ... whose host-list is not reachable. But we can invoke the predicate to
    // verify that our two hosts are in the white list.
    @SuppressWarnings("unchecked")
    Predicate<Node> predicate = (Predicate<Node>) getInternalState(dseLbp, "filter");
    assertThat(predicate.test(makeHostWithAddress("127.0.0.1"))).isTrue();
    assertThat(predicate.test(makeHostWithAddress("127.0.0.3"))).isFalse();
    assertThat(predicate.test(makeHostWithAddress("127.0.0.4"))).isTrue();

    assertThat(getInternalState(dseLbp, "localDc")).isEqualTo("myLocalDcName");
  }

  @Test
  void should_error_on_empty_hosts() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("hosts = []")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("must be provided with both auth.username and auth.password");
  }

  @Test
  void should_error_nonexistent_keytab() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "auth.provider = DseGSSAPIAuthProvider, auth.keyTab = noexist.keytab")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*Keytab file .* is not a file.*");
  }

  @Test
  void should_error_keytab_has_no_keys() throws IOException {
    Path keytabPath = Files.createTempFile("my", ".keytab");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "auth.provider = DseGSSAPIAuthProvider, auth.keyTab = "
                          + quoteJson(keytabPath))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL truststore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_truststore() throws IOException, GeneralSecurityException {
    Path truststore = Files.createTempFile("my", ".truststore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.truststore.password = mypass, ssl.truststore.path = "
                          + quoteJson(truststore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      settings.init(true, new HashMap<>());
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL keystore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_keystore() throws IOException, GeneralSecurityException {
    Path keystore = Files.createTempFile("my", ".keystore");
    try {
      LoaderConfig config =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      "ssl.keystore.password = mypass, ssl.keystore.path = " + quoteJson(keystore))
                  .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
      DriverSettings settings = new DriverSettings(config);
      settings.init(true, new HashMap<>());
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL key certificate chain file .* is not a file.*");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_accept_existing_openssl_keycertchain_and_key()
      throws IOException, GeneralSecurityException, URISyntaxException {
    Path key = Paths.get(getClass().getResource("/client.key").toURI());
    Path chain = Paths.get(getClass().getResource("/client.crt").toURI());
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
    settings.init(true, new HashMap<>());
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
      assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for driver.pooling.remote.connections: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_requests_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("pooling.requests = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for driver.pooling.requests: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_fetch_size_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.fetchSize = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'query.consistency': Expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_serial_consistency_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("query.serialConsistency = Unknown")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'query.serialConsistency': Expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_read_timeout_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("socket.readTimeout = NotADuration")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
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
    assertThatThrownBy(() -> settings.init(true, new HashMap<>()))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'pooling.heartbeat': No number in duration value 'NotADuration'");
  }

  @ParameterizedTest
  @CsvSource({
    "pooling.local.requests,1234,pooling.requests",
    "policy.lbp.dcAwareRoundRobin.localDc,testDC,policy.lbp.localDc",
    "policy.lbp.whiteList.hosts,[white-list.com],policy.lbp.allowedHosts",
  })
  void should_log_warning_when_deprecated_setting_present(
      String deprecatedSetting,
      String value,
      String newSetting,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(deprecatedSetting + " = " + value)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(true, new HashMap<>());
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Driver setting %s is deprecated; please use %s instead",
                deprecatedSetting, newSetting));
  }

  @ParameterizedTest
  @CsvSource({
    "pooling.remote.requests,1234",
    "policy.lbp.name,FooPolicy",
    "policy.lbp.dse.childPolicy,tokenAware",
    "policy.lbp.tokenAware.childPolicy,roundRobin",
    "policy.lbp.tokenAware.replicaOrdering,NEUTRAL",
    "policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel,true",
    "policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc,1",
    "policy.lbp.whiteList.childPolicy,roundRobin",
  })
  void should_log_warning_when_obsolete_setting_present(
      String setting,
      String value,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(setting + " = " + value)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(true, new HashMap<>());
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Driver setting %s is obsolete; please remove it from your configuration",
                setting));
  }

  @Test
  void should_log_info_when_cloud_and_write_and_default_CL_implicitly_changed(
      @LogCapture(level = INFO, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("cloud.secureConnectBundle = /path/to/bundle")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(true, new HashMap<>());
    assertThat(logs)
        .hasMessageContaining(
            "Changing default consistency level to LOCAL_QUORUM for Cloud deployments");
    assertThat(settings.driverConfig)
        .containsEntry(CLOUD_SECURE_CONNECT_BUNDLE, "file:/path/to/bundle");
  }

  @ParameterizedTest
  @CsvSource({"ANY", "LOCAL_ONE", "ONE"})
  void should_log_warning_when_cloud_and_write_and_incompatible_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "cloud.secureConnectBundle = /path/to/bundle, "
                        + "query.consistency = "
                        + level)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(true, new HashMap<>());
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                level));
    assertThat(settings.driverConfig)
        .containsEntry(CLOUD_SECURE_CONNECT_BUNDLE, "file:/path/to/bundle");
    assertThat(settings.driverConfig).containsEntry(REQUEST_CONSISTENCY, "LOCAL_QUORUM");
  }

  @ParameterizedTest
  @CsvSource({"TWO", "THREE", "LOCAL_QUORUM", "QUORUM", "EACH_QUORUM", "ALL"})
  void should_not_log_warning_when_cloud_and_write_and_compatible_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "cloud.secureConnectBundle = /path/to/bundle, "
                        + "query.consistency = "
                        + level)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(true, new HashMap<>());
    assertThat(logs.getLoggedEvents()).isEmpty();
    assertThat(settings.driverConfig)
        .containsEntry(CLOUD_SECURE_CONNECT_BUNDLE, "file:/path/to/bundle");
    assertThat(settings.driverConfig).containsEntry(REQUEST_CONSISTENCY, level.name());
  }

  @ParameterizedTest
  @CsvSource({
    "ANY",
    "LOCAL_ONE",
    "ONE",
    "TWO",
    "THREE",
    "LOCAL_QUORUM",
    "QUORUM",
    "EACH_QUORUM",
    "ALL"
  })
  void should_not_log_warning_when_cloud_and_read_and_any_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    "cloud.secureConnectBundle = /path/to/bundle, "
                        + "query.consistency = "
                        + level)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.driver")));
    DriverSettings settings = new DriverSettings(config);
    settings.init(false, new HashMap<>());
    assertThat(logs.getLoggedEvents()).isEmpty();
    assertThat(settings.driverConfig)
        .containsEntry(CLOUD_SECURE_CONNECT_BUNDLE, "file:/path/to/bundle");
    assertThat(settings.driverConfig).containsEntry(REQUEST_CONSISTENCY, level.name());
  }

  private static Node makeHostWithAddress(String host) {
    Node h = mock(Node.class);
    when(h.getEndPoint())
        .thenReturn(new DefaultEndPoint(InetSocketAddress.createUnresolved(host, 9042)));
    return h;
  }
}
