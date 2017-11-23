/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.settings.StringUtils.DELIMITER;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareSSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseLoadBalancingPolicy;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.internal.policies.MultipleRetryPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/** */
public class DriverSettings {

  //Path Constants
  private static final String POOLING = "pooling";
  private static final String LOCAL = "local";
  private static final String REMOTE = "remote";
  private static final String QUERY = "query";
  private static final String SOCKET = "socket";
  private static final String AUTH = "auth";
  private static final String POLICY = "policy";
  private static final String SSL = "ssl";
  private static final String PROTOCOL = "protocol";
  private static final String TRUSTSTORE = "truststore";
  private static final String KEYSTORE = "keystore";
  private static final String OPENSSL = "openssl";
  private static final String LBP = "lbp";

  private static final String HOSTS = "hosts";
  private static final String PORT = "port";
  private static final String POOLING_LOCAL_CONNECTIONS =
      POOLING + DELIMITER + LOCAL + DELIMITER + "connections";
  private static final String POOLING_REMOTE_CONNECTIONS =
      POOLING + DELIMITER + REMOTE + DELIMITER + "connections";
  private static final String POOLING_LOCAL_REQUESTS =
      POOLING + DELIMITER + LOCAL + DELIMITER + "requests";
  private static final String POOLING_REMOTE_REQUESTS =
      POOLING + DELIMITER + REMOTE + DELIMITER + "requests";
  private static final String POOLING_HEARTBEAT = POOLING + DELIMITER + "heartbeat";

  private static final String QUERY_CONSISTENCY = QUERY + DELIMITER + "consistency";
  private static final String QUERY_SERIALCONSISTENCY = QUERY + DELIMITER + "serialConsistency";
  private static final String QUERY_FETCHSIZE = QUERY + DELIMITER + "fetchSize";
  private static final String QUERY_IDEMPOTENCE = QUERY + DELIMITER + "idempotence";

  private static final String SOCKET_READTIMEOUT = SOCKET + DELIMITER + "readTimeout";
  private static final String TIMESTAMP_GENERATOR = "timestampGenerator";
  private static final String ADDRESS_TRANSLATOR = "addressTranslator";

  private static final String AUTH_PROVIDER = AUTH + DELIMITER + "provider";
  private static final String AUTH_USERNAME = AUTH + DELIMITER + "username";
  private static final String AUTH_PASSWORD = AUTH + DELIMITER + "password";
  private static final String AUTH_PRINCIPAL = AUTH + DELIMITER + "principal";
  private static final String AUTHORIZATION_ID = AUTH + DELIMITER + "authorizationId";
  private static final String AUTH_SASLPROTOCOL = AUTH + DELIMITER + "saslProtocol";
  private static final String AUTH_KEYTAB = AUTH + DELIMITER + "keyTab";

  private static final String PROTOCOL_COMPRESSION = PROTOCOL + DELIMITER + "compression";

  private static final String SSL_PROVIDER = SSL + DELIMITER + "provider";
  private static final String SSL_TRUSTSTORE_PATH =
      SSL + DELIMITER + TRUSTSTORE + DELIMITER + "path";
  private static final String SSL_TRUSTSTORE_PASSWORD =
      SSL + DELIMITER + TRUSTSTORE + DELIMITER + "password";
  private static final String SSL_KEYSTORE_PATH = SSL + DELIMITER + KEYSTORE + DELIMITER + "path";
  private static final String SSL_KEYSTORE_PASSWORD =
      SSL + DELIMITER + KEYSTORE + DELIMITER + "password";
  private static final String SSL_TRUSTSTORE_ALGORITHM =
      SSL + DELIMITER + TRUSTSTORE + DELIMITER + "algorithm";
  private static final String SSL_OPENSSL_KEYCHAINCERT =
      SSL + DELIMITER + OPENSSL + DELIMITER + "keyCertChain";
  private static final String SSL_OPENSSL_PRIVATE_KEY =
      SSL + DELIMITER + OPENSSL + DELIMITER + "privateKey";

  private static final String POLICY_NAME = POLICY + DELIMITER + LBP + DELIMITER + "name";
  private static final String POLICY_MAXRETRIES = POLICY + DELIMITER + "maxRetries";

  private static final String PLAINTEXT_PROVIDER = "PlainTextAuthProvider";
  private static final String DSE_PLAINTEXT_PROVIDER = "DsePlainTextAuthProvider";
  private static final String DSE_GSSAPI_PROVIDER = "DseGSSAPIAuthProvider";

  private final LoaderConfig config;
  private final String executionId;
  private final String hosts;
  private final int port;
  private final int poolingLocalConnections;
  private final int poolingRemoteConnections;
  private final int poolingLocalRequests;
  private final int poolingRemoteRequests;
  private final Duration poolingHeartbeat;
  private final ConsistencyLevel queryConsistency;
  private final ConsistencyLevel querySerialConsistency;
  private final int queryFetchSize;
  private final boolean queryIdempotence;
  private final Duration socketReadTimeout;
  private final TimestampGenerator timestampGenerator;
  private final AddressTranslator addressTranslator;
  private final String authProvider;
  private final int policyMaxRetries;
  private final ProtocolOptions.Compression compression;
  private String authUsername;
  private String authPrincipal;
  private String authPassword;
  private String authorizationId;
  private String sslProvider;
  private URL sslTrustStorePath;
  private String sslTrustStorePassword;
  private URL sslKeyStorePath;
  private String sslKeyStorePassword;
  private String sslTrustStoreAlgorithm;
  private URL sslOpenSslPrivateKey;
  private URL sslOpenSslKeyCertChain;
  private URL authKeyTab;
  private String authSaslProtocol;
  private LoadBalancingPolicy policy;

  DriverSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
    try {

      if (!config.hasPath("hosts")) {
        throw new BulkConfigurationException(
            "driver.hosts is mandatory. Please set driver.hosts "
                + "and try again. See settings.md or help for more information.",
            "driver");
      }
      hosts = config.getString(HOSTS);
      port = config.getInt(PORT);
      compression = config.getEnum(ProtocolOptions.Compression.class, PROTOCOL_COMPRESSION);
      poolingLocalConnections = config.getInt(POOLING_LOCAL_CONNECTIONS);
      poolingRemoteConnections = config.getInt(POOLING_REMOTE_CONNECTIONS);
      poolingLocalRequests = config.getInt(POOLING_LOCAL_REQUESTS);
      poolingRemoteRequests = config.getInt(POOLING_REMOTE_REQUESTS);
      poolingHeartbeat = config.getDuration(POOLING_HEARTBEAT);
      queryConsistency = config.getEnum(ConsistencyLevel.class, QUERY_CONSISTENCY);
      querySerialConsistency = config.getEnum(ConsistencyLevel.class, QUERY_SERIALCONSISTENCY);
      queryFetchSize = config.getInt(QUERY_FETCHSIZE);
      queryIdempotence = config.getBoolean(QUERY_IDEMPOTENCE);
      socketReadTimeout = config.getDuration(SOCKET_READTIMEOUT);
      timestampGenerator = config.getInstance(TIMESTAMP_GENERATOR);
      addressTranslator = config.getInstance(ADDRESS_TRANSLATOR);
      authProvider = config.getString(AUTH_PROVIDER);
      policyMaxRetries = config.getInt(POLICY_MAXRETRIES);
      if (!authProvider.equals("None")) {
        switch (authProvider) {
          case PLAINTEXT_PROVIDER:
          case DSE_PLAINTEXT_PROVIDER:
            if (!config.hasPath(AUTH_USERNAME) || !config.hasPath(AUTH_PASSWORD)) {
              throw new BulkConfigurationException(
                  authProvider + " must be provided with both auth.username and auth.password",
                  "driver");
            }
            authUsername = config.getString(AUTH_USERNAME);
            authPassword = config.getString(AUTH_PASSWORD);
            break;
          case DSE_GSSAPI_PROVIDER:
            if (!config.hasPath(AUTH_PRINCIPAL) || !config.hasPath(AUTH_SASLPROTOCOL)) {
              throw new BulkConfigurationException(
                  authProvider
                      + " must be provided with auth.principal and auth.saslProtocol. auth.keyTab, and auth.authorizationId are optional.",
                  "driver");
            }
            authPrincipal = config.getString(AUTH_PRINCIPAL);
            authSaslProtocol = config.getString(AUTH_SASLPROTOCOL);
            if (config.hasPath(AUTH_PRINCIPAL)) {
              authPrincipal = config.getString(AUTH_PRINCIPAL);
            }
            if (config.hasPath(AUTH_KEYTAB)) {
              authKeyTab = config.getURL(AUTH_KEYTAB);
            }

            break;
          default:
            throw new BulkConfigurationException(
                authProvider
                    + " is not a valid auth provider. Valid auth providers are PlainTextAuthProvider, DsePlainTextAuthProvider, or DseGSSAPIAuthProvider",
                "driver");
        }
      }
      sslProvider = config.getString(SSL_PROVIDER);
      if (sslProvider.equals(SSLProvider.JDK.name())) {
        if (!(config.hasPath(SSL_KEYSTORE_PATH) == config.hasPath(SSL_KEYSTORE_PASSWORD))) {
          throw new BulkConfigurationException(
              SSL_KEYSTORE_PATH
                  + ", "
                  + SSL_KEYSTORE_PASSWORD
                  + " and "
                  + SSL_TRUSTSTORE_ALGORITHM
                  + " must be provided together or not at all when using the JDK SSL Provider",
              "driver.ssl");
        } else {
          if (config.hasPath(SSL_KEYSTORE_PATH)) {
            sslKeyStorePath = config.getURL(SSL_KEYSTORE_PATH);
            sslKeyStorePassword = config.getString(SSL_KEYSTORE_PASSWORD);
            sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
          }
        }
      } else if (sslProvider.equals(SSLProvider.OpenSSL.name())) {
        if (!(config.hasPath(SSL_OPENSSL_KEYCHAINCERT)
            == config.hasPath(SSL_OPENSSL_PRIVATE_KEY))) {
          throw new BulkConfigurationException(
              SSL_OPENSSL_KEYCHAINCERT
                  + " and "
                  + SSL_OPENSSL_PRIVATE_KEY
                  + " must be provided together or not at all when using the openssl Provider",
              "driver.ssl");
        }
        if (config.hasPath(SSL_OPENSSL_KEYCHAINCERT)) {
          sslOpenSslKeyCertChain = config.getURL(SSL_OPENSSL_KEYCHAINCERT);
          sslOpenSslPrivateKey = config.getURL(SSL_OPENSSL_PRIVATE_KEY);
        }
      }
      if (!(config.hasPath(SSL_TRUSTSTORE_PATH) == config.hasPath(SSL_TRUSTSTORE_PASSWORD))) {
        throw new BulkConfigurationException(
            SSL_TRUSTSTORE_PATH
                + ", "
                + SSL_TRUSTSTORE_PASSWORD
                + " and "
                + SSL_TRUSTSTORE_ALGORITHM
                + " must be provided together or not at all",
            "driver.ssl");
      } else {
        if (config.hasPath(SSL_TRUSTSTORE_PATH)) {
          sslTrustStorePath = config.getURL(SSL_TRUSTSTORE_PATH);
          sslTrustStorePassword = config.getString(SSL_TRUSTSTORE_PASSWORD);
          sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
        }
      }
      if (config.hasPath(POLICY_NAME)) {
        policy = getLoadBalancingPolicy(config, config.getEnum(BuiltinLBP.class, POLICY_NAME));
      }
      if (config.hasPath(AUTHORIZATION_ID)) {
        authorizationId = config.getString(AUTHORIZATION_ID);
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "driver");
    }
  }

  public DseCluster newCluster() throws BulkConfigurationException {
    DseCluster.Builder builder = DseCluster.builder().withClusterName(executionId + "-driver");
    getHostsStream(hosts).forEach(builder::addContactPointsWithPorts);
    builder
        .withCompression(compression)
        .withPoolingOptions(
            new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, poolingLocalConnections)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, poolingLocalConnections)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, poolingRemoteConnections)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, poolingRemoteConnections)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, poolingLocalRequests)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, poolingRemoteRequests)
                .setHeartbeatIntervalSeconds((int) poolingHeartbeat.getSeconds()))
        .withQueryOptions(
            new QueryOptions()
                .setConsistencyLevel(queryConsistency)
                .setSerialConsistencyLevel(querySerialConsistency)
                .setFetchSize(queryFetchSize)
                .setDefaultIdempotence(queryIdempotence))
        .withSocketOptions(
            new SocketOptions().setReadTimeoutMillis((int) socketReadTimeout.toMillis()))
        .withTimestampGenerator(timestampGenerator)
        .withAddressTranslator(addressTranslator);

    if (policy != null) {
      builder.withLoadBalancingPolicy(policy);
    }

    // Configure retry-policy.
    builder.withRetryPolicy(new MultipleRetryPolicy(policyMaxRetries));

    if (!authProvider.equals("None")) {
      AuthProvider authProvider = createAuthProvider();
      builder.withAuthProvider(authProvider);
    }
    if (!sslProvider.equals("None")) {
      RemoteEndpointAwareSSLOptions sslOptions;
      try {
        sslOptions = createSSLOptions();
      } catch (Exception e) {
        throw new BulkConfigurationException("Could not configure SSL", e, "driver.ssl");
      }
      builder.withSSL(sslOptions);
    }

    return builder.build();
  }

  private LoadBalancingPolicy getLoadBalancingPolicy(LoaderConfig config, BuiltinLBP lbpName)
      throws BulkConfigurationException {
    Set<BuiltinLBP> seenPolicies = new LinkedHashSet<>();
    return getLoadBalancingPolicy(config, lbpName, seenPolicies);
  }

  private LoadBalancingPolicy getLoadBalancingPolicy(
      LoaderConfig config, BuiltinLBP lbpName, Set<BuiltinLBP> seenPolicies)
      throws BulkConfigurationException {
    LoadBalancingPolicy policy = null;
    LoadBalancingPolicy childPolicy = null;
    LoaderConfig lbpConfig = config.getConfig("policy.lbp");
    seenPolicies.add(lbpName);

    String childPolicyPath = lbpName.name() + ".childPolicy";
    if (lbpName == BuiltinLBP.dse
        || lbpName == BuiltinLBP.whiteList
        || lbpName == BuiltinLBP.tokenAware) {
      BuiltinLBP childName = lbpConfig.getEnum(BuiltinLBP.class, childPolicyPath);
      if (childName != null) {
        if (seenPolicies.contains(childName)) {
          throw new BulkConfigurationException(
              "Load balancing policy chaining loop detected: "
                  + seenPolicies.stream().map(BuiltinLBP::name).collect(Collectors.joining(","))
                  + ","
                  + childName.name(),
              "driver.policy.lbp");
        }
        childPolicy =
            getLoadBalancingPolicy(
                config, lbpConfig.getEnum(BuiltinLBP.class, childPolicyPath), seenPolicies);
      }
    }

    switch (lbpName) {
      case dse:
        policy = new DseLoadBalancingPolicy(childPolicy);
        break;
      case dcAwareRoundRobin:
        DCAwareRoundRobinPolicy.Builder builder = DCAwareRoundRobinPolicy.builder();
        builder
            .withLocalDc(lbpConfig.getString("dcAwareRoundRobin.localDc"))
            .withUsedHostsPerRemoteDc(lbpConfig.getInt("dcAwareRoundRobin.usedHostsPerRemoteDc"));
        if (lbpConfig.getBoolean("dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel")) {
          builder.allowRemoteDCsForLocalConsistencyLevel();
        }
        policy = builder.build();
        break;
      case roundRobin:
        policy = new RoundRobinPolicy();
        break;
      case whiteList:
        String WLHosts = config.getString("policy.lbp.whiteList.hosts");
        policy =
            new WhiteListPolicy(childPolicy, getHostsStream(WLHosts).collect(Collectors.toList()));

        break;
      case tokenAware:
        policy =
            new TokenAwarePolicy(childPolicy, lbpConfig.getBoolean("tokenAware.shuffleReplicas"));
        break;
    }
    return policy;
  }

  private AuthProvider createAuthProvider() {
    switch (authProvider) {
      case PLAINTEXT_PROVIDER:
        return new PlainTextAuthProvider(authUsername, authPassword);
      case DSE_PLAINTEXT_PROVIDER:
        if (config.hasPath(AUTHORIZATION_ID)) {
          return new DsePlainTextAuthProvider(authUsername, authPassword, authorizationId);
        } else {
          return new DsePlainTextAuthProvider(authUsername, authPassword);
        }
      case DSE_GSSAPI_PROVIDER:
        Configuration configuration;
        if (authKeyTab != null) {
          configuration = new KeyTabConfiguration(authPrincipal, authKeyTab.getPath());
        } else {
          configuration = new TicketCacheConfiguration(authPrincipal);
        }
        DseGSSAPIAuthProvider.Builder authProviderBuilder =
            DseGSSAPIAuthProvider.builder()
                .withLoginConfiguration(configuration)
                .withSaslProtocol(authSaslProtocol);
        if (authorizationId != null) {
          authProviderBuilder.withAuthorizationId(authorizationId);
        }
        return authProviderBuilder.build();
      default:
        throw new IllegalArgumentException("Unsupported AuthProvider: " + authProvider);
    }
  }

  private RemoteEndpointAwareSSLOptions createSSLOptions() throws Exception {

    TrustManagerFactory tmf = null;
    if (sslTrustStorePath != null) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(sslTrustStorePath.openStream(), sslTrustStorePassword.toCharArray());

      tmf = TrustManagerFactory.getInstance(config.getString("ssl.truststore.algorithm"));
      tmf.init(ks);
    }

    List<String> cipherSuites = config.getStringList("ssl.cipherSuites");

    SSLProvider sslProvider = config.getEnum(SSLProvider.class, "ssl.provider");

    switch (sslProvider) {
      case JDK:
        {
          KeyManagerFactory kmf = null;
          if (sslKeyStorePath != null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(sslKeyStorePath.openStream(), sslKeyStorePassword.toCharArray());

            kmf = KeyManagerFactory.getInstance(sslTrustStoreAlgorithm);
            kmf.init(ks, sslKeyStorePassword.toCharArray());
          }

          SSLContext sslContext = SSLContext.getInstance("TLS");
          sslContext.init(
              kmf != null ? kmf.getKeyManagers() : null,
              tmf != null ? tmf.getTrustManagers() : null,
              new SecureRandom());

          RemoteEndpointAwareJdkSSLOptions.Builder builder =
              (RemoteEndpointAwareJdkSSLOptions.Builder)
                  RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext);
          if (!cipherSuites.isEmpty()) {
            builder.withCipherSuites(cipherSuites.toArray(new String[cipherSuites.size()]));
          }
          return builder.build();
        }

      case OpenSSL:
        {
          SslContextBuilder builder =
              SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);

          if (sslOpenSslKeyCertChain != null) {
            builder.keyManager(
                sslOpenSslKeyCertChain.openStream(), sslOpenSslPrivateKey.openStream());
          }

          if (!cipherSuites.isEmpty()) {
            builder.ciphers(cipherSuites);
          }
          return new RemoteEndpointAwareNettySSLOptions(builder.build());
        }
      default:
        // cannot happen
        return null;
    }
  }

  private Stream<InetSocketAddress> getHostsStream(String hosts) {
    return Arrays.stream(hosts.split(",\\s*"))
        .map(
            s -> {
              String[] tokens = s.split(":");
              int inetPort = tokens.length > 1 ? Integer.parseInt(tokens[1]) : port;
              return new InetSocketAddress(tokens[0], inetPort);
            });
  }

  @VisibleForTesting
  static class KeyTabConfiguration extends Configuration {

    private final String principal;
    private final String keyTab;

    KeyTabConfiguration(String principal, String keyTab) {
      this.principal = principal;
      this.keyTab = keyTab;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options =
          ImmutableMap.<String, String>builder()
              .put("principal", principal)
              .put("useKeyTab", "true")
              .put("refreshKrb5Config", "true")
              .put("keyTab", keyTab)
              .build();

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)
      };
    }
  }

  @VisibleForTesting
  static class TicketCacheConfiguration extends Configuration {

    private final String principal;

    TicketCacheConfiguration(String principal) {
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options =
          ImmutableMap.<String, String>builder()
              .put("principal", principal)
              .put("useTicketCache", "true")
              .put("refreshKrb5Config", "true")
              .put("renewTGT", "true")
              .build();

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)
      };
    }
  }

  private enum BuiltinLBP {
    dse,
    dcAwareRoundRobin,
    roundRobin,
    whiteList,
    tokenAware
  }

  private enum SSLProvider {
    JDK,
    OpenSSL
  }
}
