/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareSSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.loader.commons.config.LoaderConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/** */
public class DriverSettings {

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

  public DseCluster newCluster() throws Exception {
    DseCluster.Builder builder = DseCluster.builder().withClusterName(executionId + "-driver");
    int defaultPort = config.getInt("port");
    config
        .getStringList("contactPoints")
        .forEach(
            s -> {
              String[] tokens = s.split(":");
              int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : defaultPort;
              builder.addContactPointsWithPorts(new InetSocketAddress(tokens[0], port));
            });

    ProtocolVersion protocolVersion;

    if (!config.getString("protocol.version").isEmpty()) {
      protocolVersion = config.getEnum(ProtocolVersion.class, "protocol.version");
      Preconditions.checkArgument(
          protocolVersion.compareTo(ProtocolVersion.V3) >= 0,
          "This loader does not support protocol versions lower than 3");
      builder.withProtocolVersion(protocolVersion);
    }
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

    // TODO configure policies
    if (config.hasPath("policy.lbp")) {
      builder.withLoadBalancingPolicy(config.getInstance("policy.lbp"));
    }
    if (config.hasPath("policy.retry")) {
      Class<RetryPolicy> retryPolicyClass = config.getClass("policy.retry");
      if (retryPolicyClass.equals(DefaultRetryPolicy.class)) {
        builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
      } else {
        builder.withRetryPolicy(config.getInstance("policy.retry"));
      }
    }
    if (config.hasPath("policy.specexec")) {
      Class<SpeculativeExecutionPolicy> speculativeExecutionPolicyClass =
          config.getClass("policy.specexec");
      if (speculativeExecutionPolicyClass.equals(NoSpeculativeExecutionPolicy.class)) {
        builder.withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE);
      } else {
        builder.withSpeculativeExecutionPolicy(config.getInstance("policy.specexec"));
      }
    }

    if (!config.getString("auth.provider").equals("None")) {
      AuthProvider authProvider = createAuthProvider();
      builder.withAuthProvider(authProvider);
    }
    if (!config.getString("ssl.provider").equals("None")) {
      RemoteEndpointAwareSSLOptions sslOptions = createSSLOptions();
      builder.withSSL(sslOptions);
    }

    return builder.build();
  }

  private AuthProvider createAuthProvider() throws URISyntaxException, MalformedURLException {
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
          String keyTab =
              new File(new URL(config.getString("auth.keyTab")).toURI()).getAbsolutePath();
          configuration = new KeyTabConfiguration(principal, keyTab);
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
    if (config.hasPath("ssl.truststore.url")) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new URL(config.getString("ssl.truststore.url")).openStream(),
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
          if (config.hasPath("ssl.keystore.url")) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(
                new URL(config.getString("ssl.keystore.url")).openStream(),
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
            URL keyCertChain = new URL(config.getString("ssl.openssl.keyCertChain"));
            URL privateKey = new URL(config.getString("ssl.openssl.privateKey"));
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
}
