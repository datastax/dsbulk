/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isValueFromReferenceConfig;
import static com.datastax.dsbulk.engine.internal.settings.BulkDriverOption.RETRY_POLICY_MAX_RETRIES;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.BULK_LOADER_APPLICATION_NAME;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.clientId;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.getBulkLoaderVersion;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_MAX_REQUESTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTACT_POINTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.HEARTBEAT_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_PAGE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.auth.AuthProviderFactory;
import com.datastax.dsbulk.engine.internal.ssl.SslHandlerFactoryFactory;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import com.datastax.oss.driver.internal.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.internal.core.time.ThreadLocalTimestampGenerator;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(DriverSettings.class);

  private final LoaderConfig deprecatedDriverConfig;
  private final LoaderConfig deprecatedContinuousPagingConfig;

  private LoaderConfig newDriverConfig;
  private LoaderConfig convertedConfig;
  private LoaderConfig mergedDriverConfig;

  private AuthProvider authProvider;
  @VisibleForTesting SslHandlerFactory sslHandlerFactory;
  private Predicate<Node> nodeFilter;

  DriverSettings(
      LoaderConfig deprecatedDriverConfig,
      LoaderConfig deprecatedContinuousPagingConfig,
      LoaderConfig newDriverConfig) {
    this.newDriverConfig = newDriverConfig;
    this.deprecatedDriverConfig = deprecatedDriverConfig;
    this.deprecatedContinuousPagingConfig = deprecatedContinuousPagingConfig;
  }

  public void init(boolean write) throws GeneralSecurityException, IOException {
    try {
      convertDriverDeprecatedConfig();
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(e, "dsbulk.driver");
    }
    try {
      convertContinuousPagingDeprecatedConfig(write);
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(
          e, "dsbulk.executor.continuousPaging");
    }
    mergedDriverConfig =
        new DefaultLoaderConfig(convertedConfig.withFallback(newDriverConfig).resolve());
    checkCloudCompatibility(write);
  }

  private void convertDriverDeprecatedConfig() throws GeneralSecurityException, IOException {

    convertedConfig = new DefaultLoaderConfig(ConfigFactory.empty());

    int port;
    if (isUserDefined(deprecatedDriverConfig, "port")) {
      port = deprecatedDriverConfig.getInt("port");
      warnDeprecatedSetting("dsbulk.driver.port", CONTACT_POINTS);
    } else {
      port = 9042;
    }

    if (isUserDefined(deprecatedDriverConfig, "hosts")) {
      List<String> hosts = deprecatedDriverConfig.getStringList("hosts");
      List<String> contactPoints =
          hosts.stream().map(host -> host + ':' + port).collect(Collectors.toList());
      convertedConfig = addConfigValue(convertedConfig, CONTACT_POINTS, contactPoints);
      warnDeprecatedSetting("dsbulk.driver.hosts", CONTACT_POINTS);
    }

    if (isUserDefined(deprecatedDriverConfig, "protocol.compression")) {
      String compression = deprecatedDriverConfig.getString("protocol.compression");
      switch (compression.toLowerCase()) {
        case "lz4":
        case "snappy":
        case "none":
          convertedConfig = addConfigValue(convertedConfig, PROTOCOL_COMPRESSION, compression);
          warnDeprecatedSetting("dsbulk.driver.protocol.compression", PROTOCOL_COMPRESSION);
          break;
        default:
          throw new BulkConfigurationException(
              String.format(
                  "Invalid value for dsbulk.driver.protocol.compression, expecting one of NONE, SNAPPY, LZ4, got '%s'",
                  compression));
      }
    }

    if (isUserDefined(deprecatedDriverConfig, "pooling.local.connections")) {
      int localConnections = deprecatedDriverConfig.getInt("pooling.local.connections");
      convertedConfig =
          addConfigValue(convertedConfig, CONNECTION_POOL_LOCAL_SIZE, localConnections);
      warnDeprecatedSetting("dsbulk.driver.pooling.local.connections", CONNECTION_POOL_LOCAL_SIZE);
    }

    if (isUserDefined(deprecatedDriverConfig, "pooling.remote.connections")) {
      int remoteConnections = deprecatedDriverConfig.getInt("pooling.remote.connections");
      convertedConfig =
          addConfigValue(convertedConfig, CONNECTION_POOL_REMOTE_SIZE, remoteConnections);
      warnDeprecatedSetting(
          "dsbulk.driver.pooling.remote.connections", CONNECTION_POOL_REMOTE_SIZE);
    }

    if (isUserDefined(deprecatedDriverConfig, "pooling.local.requests")) {
      int localRequests = deprecatedDriverConfig.getInt("pooling.local.requests");
      convertedConfig = addConfigValue(convertedConfig, CONNECTION_MAX_REQUESTS, localRequests);
      warnDeprecatedSetting("dsbulk.driver.pooling.local.requests", CONNECTION_MAX_REQUESTS);
    }

    if (isUserDefined(deprecatedDriverConfig, "pooling.remote.requests")) {
      deprecatedDriverConfig.getInt("pooling.remote.requests");
      // don't set CONNECTION_MAX_REQUESTS, it's already done above
      warnDeprecatedSetting("dsbulk.driver.pooling.remote.requests", CONNECTION_MAX_REQUESTS);
    }

    if (isUserDefined(deprecatedDriverConfig, "pooling.heartbeat")) {
      Duration heartbeat = deprecatedDriverConfig.getDuration("pooling.heartbeat");
      convertedConfig = addConfigValue(convertedConfig, HEARTBEAT_INTERVAL, heartbeat);
      warnDeprecatedSetting("dsbulk.driver.pooling.heartbeat", HEARTBEAT_INTERVAL);
    }

    if (isUserDefined(deprecatedDriverConfig, "query.consistency")) {
      ConsistencyLevel consistency =
          deprecatedDriverConfig.getEnum(DefaultConsistencyLevel.class, "query.consistency");
      convertedConfig = addConfigValue(convertedConfig, REQUEST_CONSISTENCY, consistency.name());
      warnDeprecatedSetting("dsbulk.driver.query.consistency", REQUEST_CONSISTENCY);
    }

    if (isUserDefined(deprecatedDriverConfig, "query.serialConsistency")) {
      ConsistencyLevel consistency =
          deprecatedDriverConfig.getEnum(DefaultConsistencyLevel.class, "query.serialConsistency");
      convertedConfig =
          addConfigValue(convertedConfig, REQUEST_SERIAL_CONSISTENCY, consistency.name());
      warnDeprecatedSetting("dsbulk.driver.query.serialConsistency", REQUEST_SERIAL_CONSISTENCY);
    }

    if (isUserDefined(deprecatedDriverConfig, "query.fetchSize")) {
      int fetchSize = deprecatedDriverConfig.getInt("query.fetchSize");
      convertedConfig = addConfigValue(convertedConfig, REQUEST_PAGE_SIZE, fetchSize);
      warnDeprecatedSetting("dsbulk.driver.query.fetchSize", REQUEST_PAGE_SIZE);
    }

    if (isUserDefined(deprecatedDriverConfig, "query.idempotence")) {
      boolean idempotence = deprecatedDriverConfig.getBoolean("query.idempotence");
      convertedConfig = addConfigValue(convertedConfig, REQUEST_DEFAULT_IDEMPOTENCE, idempotence);
      warnDeprecatedSetting("dsbulk.driver.query.idempotence", REQUEST_DEFAULT_IDEMPOTENCE);
    }

    if (isUserDefined(deprecatedDriverConfig, "socket.readTimeout")) {
      Duration readTimeout = deprecatedDriverConfig.getDuration("socket.readTimeout");
      convertedConfig = addConfigValue(convertedConfig, REQUEST_TIMEOUT, readTimeout);
      // also modify continuous paging accordingly, to emulate old behavior
      convertedConfig =
          addConfigValue(convertedConfig, CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, readTimeout);
      convertedConfig =
          addConfigValue(convertedConfig, CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, readTimeout);
      warnDeprecatedSetting("dsbulk.driver.socket.readTimeout", REQUEST_TIMEOUT);
    }

    if (isUserDefined(deprecatedDriverConfig, "auth")) {
      authProvider =
          AuthProviderFactory.createAuthProvider(deprecatedDriverConfig.getConfig("auth"));
      warnDeprecatedSetting("dsbulk.driver.auth.*", "advanced.auth-provider.*");
    } else if (!newDriverConfig.hasPath("advanced.auth-provider.class")
        && newDriverConfig.hasPath("advanced.auth-provider.username")
        && newDriverConfig.hasPath("advanced.auth-provider.password")) {
      // Emulate DSBulk behavior for legacy auth settings: when username and password are set but no
      // auth provider class is set, infer DsePlainTextAuthProvider
      LOGGER.info(
          "Username and password provided but auth provider not specified, inferring DsePlainTextAuthProvider");
      newDriverConfig =
          newDriverConfig.withValue(
              "advanced.auth-provider.class",
              ConfigValueFactory.fromAnyRef(DsePlainTextAuthProvider.class.getSimpleName()));
    }

    if (isUserDefined(deprecatedDriverConfig, "ssl")) {
      sslHandlerFactory =
          SslHandlerFactoryFactory.createSslHandlerFactory(deprecatedDriverConfig.getConfig("ssl"));
      warnDeprecatedSetting("dsbulk.driver.ssl.*", "advanced.ssl-engine-factory.*");
    }

    if (isUserDefined(deprecatedDriverConfig, "timestampGenerator")) {
      String generator = deprecatedDriverConfig.getString("timestampGenerator");
      Class<? extends TimestampGenerator> generatorClass;
      switch (generator) {
        case "AtomicMonotonicTimestampGenerator":
          generatorClass = AtomicTimestampGenerator.class;
          break;
        case "ThreadLocalTimestampGenerator":
          generatorClass = ThreadLocalTimestampGenerator.class;
          break;
        case "ServerSideTimestampGenerator":
          generatorClass = ServerSideTimestampGenerator.class;
          break;
        default:
          // since this setting is now deprecated, we only support built-in values,
          // dynamic loading of user-provided classes is not supported anymore
          throw new BulkConfigurationException(
              String.format(
                  "Invalid value for dsbulk.driver.protocol.timestampGenerator, "
                      + "expecting one of AtomicMonotonicTimestampGenerator, "
                      + "ThreadLocalTimestampGenerator, ServerSideTimestampGenerator, got '%s'",
                  generator));
      }
      convertedConfig =
          addConfigValue(
              convertedConfig, TIMESTAMP_GENERATOR_CLASS, generatorClass.getSimpleName());
      warnDeprecatedSetting("dsbulk.driver.timestampGenerator", TIMESTAMP_GENERATOR_CLASS);
    }

    if (isUserDefined(deprecatedDriverConfig, "addressTranslator")) {
      String translator = deprecatedDriverConfig.getString("addressTranslator");
      // since this setting is now deprecated, we only support the single built-in value
      // IdentityTranslator, dynamic loading of user-provided classes is not supported anymore
      if (!translator.equals("IdentityTranslator")) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for dsbulk.driver.protocol.addressTranslator, "
                    + "expecting IdentityTranslator, got '%s'",
                translator));
      }
      convertedConfig = addConfigValue(convertedConfig, ADDRESS_TRANSLATOR_CLASS, translator);
      warnDeprecatedSetting("dsbulk.driver.addressTranslator", ADDRESS_TRANSLATOR_CLASS);
    }

    if (isUserDefined(deprecatedDriverConfig, "policy.maxRetries")) {
      int maxRetries = deprecatedDriverConfig.getInt("policy.maxRetries");
      convertedConfig = addConfigValue(convertedConfig, RETRY_POLICY_MAX_RETRIES, maxRetries);
      warnDeprecatedSetting("dsbulk.driver.policy.maxRetries", RETRY_POLICY_MAX_RETRIES);
    }

    if (isUserDefined(deprecatedDriverConfig, "policy.lbp")) {

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.name")) {
        warnObsoleteLBPSetting("name");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.dse.childPolicy")) {
        warnObsoleteLBPSetting("dse.childPolicy");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.dcAwareRoundRobin.localDc")) {
        String localDc = deprecatedDriverConfig.getString("policy.lbp.dcAwareRoundRobin.localDc");
        convertedConfig = addConfigValue(convertedConfig, LOAD_BALANCING_LOCAL_DATACENTER, localDc);
        warnDeprecatedSetting(
            "dsbulk.driver.policy.lbp.dcAwareRoundRobin.localDc", LOAD_BALANCING_LOCAL_DATACENTER);
      }

      if (isUserDefined(
          deprecatedDriverConfig,
          "policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel")) {
        warnObsoleteLBPSetting("dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel");
      }

      if (isUserDefined(
          deprecatedDriverConfig, "policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc")) {
        warnObsoleteLBPSetting("dcAwareRoundRobin.usedHostsPerRemoteDc");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.tokenAware.childPolicy")) {
        warnObsoleteLBPSetting("tokenAware.childPolicy");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.tokenAware.replicaOrdering")) {
        warnObsoleteLBPSetting("tokenAware.replicaOrdering");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.whiteList.childPolicy")) {
        warnObsoleteLBPSetting("whiteList.childPolicy");
      }

      if (isUserDefined(deprecatedDriverConfig, "policy.lbp.whiteList.hosts")) {
        List<String> whiteList = deprecatedDriverConfig.getStringList("policy.lbp.whiteList.hosts");
        if (!whiteList.isEmpty()) {
          List<SocketAddress> allowedHosts =
              whiteList.stream()
                  .flatMap(
                      host -> {
                        try {
                          return Arrays.stream(InetAddress.getAllByName(host));
                        } catch (UnknownHostException e) {
                          String msg =
                              String.format(
                                  "Could not resolve host: %s, please verify your %s setting",
                                  host, "policy.lbp.whiteList.hosts");
                          throw new BulkConfigurationException(msg, e);
                        }
                      })
                  .map(host -> new InetSocketAddress(host, port))
                  .collect(ImmutableList.toImmutableList());
          nodeFilter = node -> allowedHosts.contains(node.getEndPoint().resolve());
        }
        warnDeprecatedSetting(
            "dsbulk.driver.policy.lbp.whiteList.hosts", LOAD_BALANCING_FILTER_CLASS);
      }
    }
  }

  private void convertContinuousPagingDeprecatedConfig(boolean write) {
    if (!write && deprecatedContinuousPagingConfig.hasPath("enabled")) {

      boolean continuousPagingEnabled = deprecatedContinuousPagingConfig.getBoolean("enabled");

      if (continuousPagingEnabled) {

        if (isUserDefined(deprecatedContinuousPagingConfig, "pageSize")) {
          int pageSize = deprecatedContinuousPagingConfig.getInt("pageSize");
          convertedConfig = addConfigValue(convertedConfig, CONTINUOUS_PAGING_PAGE_SIZE, pageSize);
          warnDeprecatedSetting(
              "dsbulk.executor.continuousPaging.pageSize", CONTINUOUS_PAGING_PAGE_SIZE);
        }

        if (isUserDefined(deprecatedContinuousPagingConfig, "pageUnit")) {
          String pageUnit = deprecatedContinuousPagingConfig.getString("pageUnit");
          switch (pageUnit.toLowerCase()) {
            case "bytes":
            case "rows":
              convertedConfig =
                  addConfigValue(
                      convertedConfig,
                      CONTINUOUS_PAGING_PAGE_SIZE_BYTES,
                      "bytes".equalsIgnoreCase(pageUnit));
              warnDeprecatedSetting(
                  "dsbulk.executor.continuousPaging.pageUnit", CONTINUOUS_PAGING_PAGE_SIZE_BYTES);
              break;
            default:
              throw new BulkConfigurationException(
                  String.format(
                      "Invalid value for dsbulk.executor.continuousPaging.pageUnit, "
                          + "expecting one of BYTES, ROWS, got '%s'",
                      pageUnit));
          }
        }

        if (isUserDefined(deprecatedContinuousPagingConfig, "maxPages")) {
          int maxPages = deprecatedContinuousPagingConfig.getInt("maxPages");
          convertedConfig = addConfigValue(convertedConfig, CONTINUOUS_PAGING_MAX_PAGES, maxPages);
          warnDeprecatedSetting(
              "dsbulk.executor.continuousPaging.maxPages", CONTINUOUS_PAGING_MAX_PAGES);
        }

        if (isUserDefined(deprecatedContinuousPagingConfig, "maxPagesPerSecond")) {
          int maxPagesPerSecond = deprecatedContinuousPagingConfig.getInt("maxPagesPerSecond");
          convertedConfig =
              addConfigValue(
                  convertedConfig, CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, maxPagesPerSecond);
          warnDeprecatedSetting(
              "dsbulk.executor.continuousPaging.maxPagesPerSecond",
              CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND);
        }
      }
    }
  }

  private void checkCloudCompatibility(boolean write) {
    boolean cloud = mergedDriverConfig.hasPath(CLOUD_SECURE_CONNECT_BUNDLE.getPath());
    if (cloud) {
      if (mergedDriverConfig.hasPath(CONTACT_POINTS.getPath())) {
        if (isValueFromReferenceConfig(mergedDriverConfig, CONTACT_POINTS.getPath())) {
          LOGGER.info(
              "A cloud secure connect bundle was provided: ignoring all explicit contact points.");
        } else {
          List<String> contactPoints = mergedDriverConfig.getStringList(CONTACT_POINTS.getPath());
          if (!contactPoints.isEmpty()) {
            LOGGER.warn(
                "Explicit contact points provided together with a cloud secure connect bundle; "
                    + "the following contact points will be ignored: \"{}\"",
                Joiner.on("\", \"").join(contactPoints));
          }
        }
        mergedDriverConfig =
            mergedDriverConfig.withValue(
                CONTACT_POINTS.getPath(), ConfigValueFactory.fromAnyRef(Collections.emptyList()));
      }
      if (mergedDriverConfig.hasPath(REQUEST_CONSISTENCY.getPath())) {
        ConsistencyLevel cl =
            mergedDriverConfig.getEnum(
                DefaultConsistencyLevel.class, REQUEST_CONSISTENCY.getPath());
        if (!isCLCloudCompatible(write, cl)) {
          if (isValueFromReferenceConfig(mergedDriverConfig, REQUEST_CONSISTENCY.getPath())) {
            LOGGER.info(
                "A cloud secure connect bundle was provided and selected operation performs writes: "
                    + "changing default consistency level to LOCAL_QUORUM.");
          } else {
            LOGGER.warn(
                "A cloud secure connect bundle was provided together with consistency level {}, "
                    + "but selected operation performs writes: "
                    + "forcing default consistency level to LOCAL_QUORUM.",
                cl);
          }
          mergedDriverConfig =
              mergedDriverConfig.withValue(
                  REQUEST_CONSISTENCY.getPath(), ConfigValueFactory.fromAnyRef("LOCAL_QUORUM"));
        }
      }
      if (sslHandlerFactory != null
          || mergedDriverConfig.hasPath("advanced.ssl-engine-factory.class")) {
        LOGGER.warn(
            "Explicit SSL configuration provided together with a cloud secure connect bundle: "
                + "SSL settings will be ignored.");
        mergedDriverConfig = mergedDriverConfig.withoutPath("advanced.ssl-engine-factory");
        sslHandlerFactory = null;
      }
    }
  }

  public Config getDriverConfig() {
    return mergedDriverConfig;
  }

  public DseSession newSession(String executionId) {
    DseSessionBuilder sessionBuilder =
        new BulkLoaderSessionBuilder()
            .withApplicationVersion(getBulkLoaderVersion())
            .withApplicationName(BULK_LOADER_APPLICATION_NAME + " " + executionId)
            .withClientId(clientId(executionId))
            .withAuthProvider(authProvider)
            .withConfigLoader(new BulkLoaderDriverConfigLoader());
    if (nodeFilter != null) {
      sessionBuilder.withNodeFilter(nodeFilter);
    }
    return sessionBuilder.build();
  }

  private static LoaderConfig addConfigValue(
      LoaderConfig converted, DriverOption option, Object value) {
    return converted.withValue(option.getPath(), ConfigValueFactory.fromAnyRef(value));
  }

  private static boolean isUserDefined(LoaderConfig config, String path) {
    return config.hasPath(path) && !isValueFromReferenceConfig(config, path);
  }

  private static void warnDeprecatedSetting(String deprecated, DriverOption replacement) {
    warnDeprecatedSetting(deprecated, replacement.getPath());
  }

  private static void warnDeprecatedSetting(String deprecated, String replacement) {
    LOGGER.warn(
        "Setting {} is deprecated and will be removed in a future release; "
            + "please configure the driver directly using datastax-java-driver.{} instead.",
        deprecated,
        replacement);
  }

  private static void warnObsoleteLBPSetting(String deprecated) {
    LOGGER.warn(
        "Setting dsbulk.driver.policy.lbp.{} has been removed and is not honored anymore; "
            + "please remove it from your configuration. "
            + "To configure the load balancing policy, use datastax-java-driver.basic.load-balancing-policy.* instead",
        deprecated);
  }

  private static boolean isCLCloudCompatible(boolean write, ConsistencyLevel cl) {
    if (write) {
      int protocolCode = cl.getProtocolCode();
      return protocolCode != ProtocolConstants.ConsistencyLevel.ANY
          && protocolCode != ProtocolConstants.ConsistencyLevel.ONE
          && protocolCode != ProtocolConstants.ConsistencyLevel.LOCAL_ONE;
    } else {
      // All levels accepted when reading
      return true;
    }
  }

  private class BulkLoaderDriverConfigLoader extends DefaultDriverConfigLoader {
    BulkLoaderDriverConfigLoader() {
      super(DriverSettings.this::getDriverConfig);
    }

    @Override
    public boolean supportsReloading() {
      return false;
    }
  }

  private class BulkLoaderSessionBuilder extends DseSessionBuilder {
    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      return new DseDriverContext(
          configLoader, programmaticArguments, dseProgrammaticArgumentsBuilder.build()) {

        @Override
        protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
          // If a custom SSL handler factory was created from deprecated config, use it;
          // otherwise fall back to regular driver settings.
          if (sslHandlerFactory == null) {
            return getSslEngineFactory().map(JdkSslHandlerFactory::new);
          } else {
            return Optional.of(sslHandlerFactory);
          }
        }
      };
    }
  }
}
