/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

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
import com.datastax.dsbulk.engine.WorkflowType;
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
public class DriverSettings implements SettingsValidator {

  enum SSLProvider {
    JDK,
    OpenSSL
  }

  private final LoaderConfig config;

  private final String executionId;

  DriverSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public DseCluster newCluster() throws BulkConfigurationException {
    DseCluster.Builder builder = DseCluster.builder().withClusterName(executionId + "-driver");
    getHostsStream(config, "hosts").forEach(builder::addContactPointsWithPorts);

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
        .withTimestampGenerator(config.getInstance("timestampGenerator"))
        .withAddressTranslator(config.getInstance("addressTranslator"));

    if (config.hasPath("policy.lbp.name")) {
      builder.withLoadBalancingPolicy(
          getLoadBalancingPolicy(config, config.getEnum(BuiltinLBP.class, "policy.lbp.name")));
    }

    // Configure retry-policy.
    builder.withRetryPolicy(new MultipleRetryPolicy(config.getInt("policy.maxRetries")));

    if (!config.getString("auth.provider").equals("None")) {
      AuthProvider authProvider = createAuthProvider();
      builder.withAuthProvider(authProvider);
    }
    if (!config.getString("ssl.provider").equals("None")) {
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
        policy =
            new WhiteListPolicy(
                childPolicy,
                getHostsStream(config, "policy.lbp.whiteList.hosts").collect(Collectors.toList()));
        break;
      case tokenAware:
        policy =
            new TokenAwarePolicy(childPolicy, lbpConfig.getBoolean("tokenAware.shuffleReplicas"));
        break;
    }
    return policy;
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getInt("port");
      config.getEnum(ProtocolOptions.Compression.class, "protocol.compression");

      if (!config.hasPath("hosts")) {
        throw new BulkConfigurationException(
            "driver.hosts is mandatory. Please set driver.hosts "
                + "and try again. See settings.md or help for more information.",
            "driver");
      }
      config.getString("hosts");
      config.getInt("pooling.local.connections");
      config.getInt("pooling.remote.connections");
      config.getInt("pooling.local.requests");
      config.getInt("pooling.remote.requests");
      config.getDuration("pooling.heartbeat");
      config.getEnum(ConsistencyLevel.class, "query.consistency");
      config.getEnum(ConsistencyLevel.class, "query.serialConsistency");
      config.getInt("query.fetchSize");
      config.getBoolean("query.idempotence");
      config.getDuration("socket.readTimeout");
      config.getInstance("timestampGenerator");
      config.getInstance("addressTranslator");
      config.getString("auth.provider");
      if (!config.getString("auth.provider").equals("None")) {
        String authProviderName = config.getString("auth.provider");
        switch (authProviderName) {
          case "PlainTextAuthProvider":
          case "DsePlainTextAuthProvider":
            if (!config.hasPath("auth.username") || !config.hasPath("auth.password")) {
              throw new BulkConfigurationException(
                  authProviderName + " must be provided with both auth.username and auth.password",
                  "driver");
            }
            break;
          case "DseGSSAPIAuthProvider":
            if (!config.hasPath("auth.principal") || !config.hasPath("auth.saslProtocol")) {
              throw new BulkConfigurationException(
                  authProviderName
                      + " must be provided with auth.principal and auth.saslProtocol. auth.keyTab, and auth.authorizationId are optional.",
                  "driver");
            }
            break;
          default:
            throw new BulkConfigurationException(
                authProviderName
                    + " is not a valid auth provider. Valid auth providers are PlainTextAuthProvider, DsePlainTextAuthProvider, or DseGSSAPIAuthProvider",
                "driver");
        }
      }
      if (config.hasPath("policy.name")) {
        getLoadBalancingPolicy(config, config.getEnum(BuiltinLBP.class, "policy.name"));
      }
      config.getInt("policy.maxRetries");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "driver");
    }
  }

  private AuthProvider createAuthProvider() {
    String authProviderName = config.getString("auth.provider");
    switch (authProviderName) {
      case "PlainTextAuthProvider":
        return new PlainTextAuthProvider(
            config.getString("auth.username"), config.getString("auth.password"));
      case "DsePlainTextAuthProvider":
        if (config.hasPath("auth.authorizationId")) {
          return new DsePlainTextAuthProvider(
              config.getString("auth.username"),
              config.getString("auth.password"),
              config.getString("auth.authorizationId"));
        } else {
          return new DsePlainTextAuthProvider(
              config.getString("auth.username"), config.getString("auth.password"));
        }
      case "DseGSSAPIAuthProvider":
        String principal = config.getString("auth.principal");
        Configuration configuration;
        if (config.hasPath("auth.keyTab")) {
          URL keyTab = config.getURL("auth.keyTab");
          configuration = new KeyTabConfiguration(principal, keyTab.getPath());
        } else {
          configuration = new TicketCacheConfiguration(principal);
        }
        DseGSSAPIAuthProvider.Builder authProviderBuilder =
            DseGSSAPIAuthProvider.builder()
                .withLoginConfiguration(configuration)
                .withSaslProtocol(config.getString("auth.saslProtocol"));
        if (config.hasPath("auth.authorizationId")) {
          authProviderBuilder.withAuthorizationId(config.getString("auth.authorizationId"));
        }
        return authProviderBuilder.build();
      default:
        throw new IllegalArgumentException("Unsupported AuthProvider: " + authProviderName);
    }
  }

  private RemoteEndpointAwareSSLOptions createSSLOptions() throws Exception {

    TrustManagerFactory tmf = null;
    if (config.hasPath("ssl.truststore.path")) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          config.getURL("ssl.truststore.path").openStream(),
          config.getString("ssl.truststore.password").toCharArray());

      tmf = TrustManagerFactory.getInstance(config.getString("ssl.truststore.algorithm"));
      tmf.init(ks);
    }

    List<String> cipherSuites = config.getStringList("ssl.cipherSuites");

    SSLProvider sslProvider = config.getEnum(SSLProvider.class, "ssl.provider");

    switch (sslProvider) {
      case JDK:
        {
          KeyManagerFactory kmf = null;
          if (config.hasPath("ssl.keystore.path")) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(
                config.getURL("ssl.keystore.path").openStream(),
                config.getString("ssl.keystore.password").toCharArray());

            kmf = KeyManagerFactory.getInstance(config.getString("ssl.truststore.algorithm"));
            kmf.init(ks, config.getString("ssl.keystore.password").toCharArray());
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

          if (config.hasPath("ssl.openssl.keyCertChain")) {
            URL keyCertChain = config.getURL("ssl.openssl.keyCertChain");
            URL privateKey = config.getURL("ssl.openssl.privateKey");
            builder.keyManager(keyCertChain.openStream(), privateKey.openStream());
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

  private Stream<InetSocketAddress> getHostsStream(LoaderConfig config, String path) {
    int defaultPort = config.getInt("port");
    return Arrays.stream(config.getString(path).split(",\\s*"))
        .map(
            s -> {
              String[] tokens = s.split(":");
              int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : defaultPort;
              return new InetSocketAddress(tokens[0], port);
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
}
