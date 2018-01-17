/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.utils.StringUtils.DELIMITER;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.assertAccessibleFile;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.CodecRegistry;
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
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Path;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class DriverSettings {
  private static final Logger LOGGER = LoggerFactory.getLogger(DriverSettings.class);

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
  private static final String AUTH_SASL_SERVICE = AUTH + DELIMITER + "saslService";
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
  private static final String SSL_OPENSSL_KEYCERTCHAIN =
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
  private String hosts;
  private int port;
  private int poolingLocalConnections;
  private int poolingRemoteConnections;
  private int poolingLocalRequests;
  private int poolingRemoteRequests;
  private Duration poolingHeartbeat;
  private ConsistencyLevel queryConsistency;
  private ConsistencyLevel querySerialConsistency;
  private int queryFetchSize;
  private boolean queryIdempotence;
  private Duration socketReadTimeout;
  private TimestampGenerator timestampGenerator;
  private AddressTranslator addressTranslator;
  private String authProvider;
  private int policyMaxRetries;
  private ProtocolOptions.Compression compression;
  private String authUsername;
  private String authPrincipal;
  private String authPassword;
  private String authorizationId;
  private String sslProvider;
  private Path sslTrustStorePath;
  private String sslTrustStorePassword;
  private Path sslKeyStorePath;
  private String sslKeyStorePassword;
  private String sslTrustStoreAlgorithm;
  private Path sslOpenSslPrivateKey;
  private Path sslOpenSslKeyCertChain;
  private Path authKeyTab;
  private String authSaslService;
  private LoadBalancingPolicy policy;

  DriverSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init() {
    try {
      if (!config.hasPath("hosts")) {
        throw new BulkConfigurationException(
            "driver.hosts is mandatory. Please set driver.hosts "
                + "and try again. See settings.md or help for more information.");
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
                  String.format(
                      "%s must be provided with both %s and %s",
                      authProvider, AUTH_USERNAME, AUTH_PASSWORD));
            }
            authUsername = config.getString(AUTH_USERNAME);
            authPassword = config.getString(AUTH_PASSWORD);
            break;
          case DSE_GSSAPI_PROVIDER:
            if (!config.hasPath(AUTH_SASL_SERVICE)) {
              throw new BulkConfigurationException(
                  String.format(
                      "%s must be provided with %s. %s, %s, and %s are optional.",
                      authProvider,
                      AUTH_SASL_SERVICE,
                      AUTH_PRINCIPAL,
                      AUTH_KEYTAB,
                      AUTHORIZATION_ID));
            }
            if (config.hasPath(AUTH_PRINCIPAL)) {
              authPrincipal = config.getString(AUTH_PRINCIPAL);
            }
            if (config.hasPath(AUTH_KEYTAB)) {
              authKeyTab = config.getPath(AUTH_KEYTAB);
              assertAccessibleFile(authKeyTab, "Keytab file");

              // When using a keytab, we must have a principal. If the user didn't provide one,
              // try to get the first principal from the keytab.
              if (authPrincipal == null) {
                // Best effort: get the first principal in the keytab, if possible.
                // We use reflection because we're referring to sun internal kerberos classes:
                // sun.security.krb5.internal.ktab.KeyTab;
                // sun.security.krb5.internal.ktab.KeyTabEntry;
                // The code below is equivalent to the following:
                //
                // keyTab = KeyTab.getInstance(authKeyTab.toString());
                // KeyTabEntry[] entries = keyTab.getEntries();
                // if (entries.length > 0) {
                //   authPrincipal = entries[0].getService().getName();
                //   LOGGER.debug("Found Kerberos principal %s in %s", authPrincipal, authKeyTab);
                // } else {
                //   throw new BulkConfigurationException(
                //   String.format("Could not find any principals in %s", authKeyTab));
                // }

                try {
                  Class<?> keyTabClazz = Class.forName("sun.security.krb5.internal.ktab.KeyTab");
                  Class<?> keyTabEntryClazz =
                      Class.forName("sun.security.krb5.internal.ktab.KeyTabEntry");
                  Class<?> principalNameClazz = Class.forName("sun.security.krb5.PrincipalName");

                  Method getInstanceMethod = keyTabClazz.getMethod("getInstance", String.class);
                  Method getEntriesMethod = keyTabClazz.getMethod("getEntries");
                  Method getServiceMethod = keyTabEntryClazz.getMethod("getService");
                  Method getNameMethod = principalNameClazz.getMethod("getName");

                  Object keyTab = getInstanceMethod.invoke(null, authKeyTab.toString());
                  Object[] entries = (Object[]) getEntriesMethod.invoke(keyTab);

                  if (entries.length > 0) {
                    authPrincipal =
                        (String) getNameMethod.invoke(getServiceMethod.invoke(entries[0]));
                    LOGGER.debug("Found Kerberos principal %s in %s", authPrincipal, authKeyTab);
                  } else {
                    throw new BulkConfigurationException(
                        String.format("Could not find any principals in %s", authKeyTab));
                  }
                } catch (ClassNotFoundException
                    | NoSuchMethodException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                  throw new BulkConfigurationException(
                      String.format("Could not find any principals in %s", authKeyTab), e);
                }
              }
            }
            authSaslService = config.getString(AUTH_SASL_SERVICE);

            break;
          default:
            throw new BulkConfigurationException(
                String.format(
                    "%s is not a valid auth provider. Valid auth providers are %s, %s, or %s",
                    authProvider, PLAINTEXT_PROVIDER, DSE_PLAINTEXT_PROVIDER, DSE_GSSAPI_PROVIDER));
        }
      }
      sslProvider = config.getString(SSL_PROVIDER);
      if (sslProvider.equals(SSLProvider.JDK.name())) {
        if (config.hasPath(SSL_KEYSTORE_PATH) != config.hasPath(SSL_KEYSTORE_PASSWORD)) {
          throw new BulkConfigurationException(
              SSL_KEYSTORE_PATH
                  + ", "
                  + SSL_KEYSTORE_PASSWORD
                  + " and "
                  + SSL_TRUSTSTORE_ALGORITHM
                  + " must be provided together or not at all when using the JDK SSL Provider");
        } else {
          if (config.hasPath(SSL_KEYSTORE_PATH)) {
            sslKeyStorePath = config.getPath(SSL_KEYSTORE_PATH);
            assertAccessibleFile(sslKeyStorePath, "SSL keystore file");
            sslKeyStorePassword = config.getString(SSL_KEYSTORE_PASSWORD);
            sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
          }
        }
      } else if (sslProvider.equals(SSLProvider.OpenSSL.name())) {
        if (config.hasPath(SSL_OPENSSL_KEYCERTCHAIN) != config.hasPath(SSL_OPENSSL_PRIVATE_KEY)) {
          throw new BulkConfigurationException(
              SSL_OPENSSL_KEYCERTCHAIN
                  + " and "
                  + SSL_OPENSSL_PRIVATE_KEY
                  + " must be provided together or not at all when using the openssl Provider");
        }
        if (config.hasPath(SSL_OPENSSL_KEYCERTCHAIN)) {
          sslOpenSslKeyCertChain = config.getPath(SSL_OPENSSL_KEYCERTCHAIN);
          sslOpenSslPrivateKey = config.getPath(SSL_OPENSSL_PRIVATE_KEY);
          assertAccessibleFile(sslOpenSslKeyCertChain, "OpenSSL key certificate chain file");
          assertAccessibleFile(sslOpenSslPrivateKey, "OpenSSL private key file");
        }
      }
      if (!sslProvider.equals(SSLProvider.None.name())) {
        if (config.hasPath(SSL_TRUSTSTORE_PATH) != config.hasPath(SSL_TRUSTSTORE_PASSWORD)) {
          throw new BulkConfigurationException(
              SSL_TRUSTSTORE_PATH
                  + ", "
                  + SSL_TRUSTSTORE_PASSWORD
                  + " and "
                  + SSL_TRUSTSTORE_ALGORITHM
                  + " must be provided together or not at all");
        } else {
          if (config.hasPath(SSL_TRUSTSTORE_PATH)) {
            sslTrustStorePath = config.getPath(SSL_TRUSTSTORE_PATH);
            assertAccessibleFile(sslTrustStorePath, "SSL truststore file");
            sslTrustStorePassword = config.getString(SSL_TRUSTSTORE_PASSWORD);
            sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
          }
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
        .withCodecRegistry(new CodecRegistry())
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
        .withAddressTranslator(addressTranslator)
        // driver metrics will be incorporated into DSBulk JMX reporting
        .withoutJMXReporting();

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
        throw new BulkConfigurationException("Could not configure SSL", e);
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
                  + childName.name());
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
          configuration = new KeyTabConfiguration(authPrincipal, authKeyTab.toString());
        } else {
          configuration = new TicketCacheConfiguration(authPrincipal);
        }
        DseGSSAPIAuthProvider.Builder authProviderBuilder =
            DseGSSAPIAuthProvider.builder()
                .withLoginConfiguration(configuration)
                .withSaslProtocol(authSaslService);
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
      ks.load(
          new BufferedInputStream(new FileInputStream(sslTrustStorePath.toFile())),
          sslTrustStorePassword.toCharArray());

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
            ks.load(
                new BufferedInputStream(new FileInputStream(sslKeyStorePath.toFile())),
                sslKeyStorePassword.toCharArray());

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
                new BufferedInputStream(new FileInputStream(sslOpenSslKeyCertChain.toFile())),
                new BufferedInputStream(new FileInputStream(sslOpenSslPrivateKey.toFile())));
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
      ImmutableMap.Builder<String, String> builder =
          ImmutableMap.<String, String>builder()
              .put("useTicketCache", "true")
              .put("refreshKrb5Config", "true")
              .put("renewTGT", "true");

      if (principal != null) {
        builder.put("principal", principal);
      }

      Map<String, String> options = builder.build();

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
    None,
    JDK,
    OpenSSL
  }
}
