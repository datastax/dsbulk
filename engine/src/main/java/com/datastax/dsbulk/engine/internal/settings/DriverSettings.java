/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.BULK_LOADER_APPLICATION_NAME;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.clientId;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.getBulkLoaderVersion;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_MAX_REQUESTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTACT_POINTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.HEARTBEAT_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_PAGE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RETRY_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS;
import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_ROOT_PATH;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.internal.auth.AuthProviderFactory;
import com.datastax.dsbulk.engine.internal.policies.lbp.DCInferringDseLoadBalancingPolicy;
import com.datastax.dsbulk.engine.internal.policies.retry.MultipleRetryPolicy;
import com.datastax.dsbulk.engine.internal.ssl.SslHandlerFactoryFactory;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.internal.core.config.typesafe.DefaultDseDriverConfigLoader;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DriverSettings {

  // Path Constants
  private static final String POOLING = "pooling";
  private static final String LOCAL = "local";
  private static final String REMOTE = "remote";
  private static final String QUERY = "query";
  private static final String SOCKET = "socket";
  private static final String POLICY = "policy";
  private static final String PROTOCOL = "protocol";
  private static final String LBP = "lbp";

  private static final String HOSTS = "hosts";
  private static final String PORT = "port";
  private static final String POOLING_LOCAL_CONNECTIONS =
      POOLING + '.' + LOCAL + '.' + "connections";
  private static final String POOLING_REMOTE_CONNECTIONS =
      POOLING + '.' + REMOTE + '.' + "connections";
  private static final String POOLING_REQUESTS = POOLING + '.' + "requests";
  private static final String POOLING_HEARTBEAT = POOLING + '.' + "heartbeat";

  private static final String QUERY_CONSISTENCY = QUERY + '.' + "consistency";
  private static final String QUERY_SERIALCONSISTENCY = QUERY + '.' + "serialConsistency";
  private static final String QUERY_FETCHSIZE = QUERY + '.' + "fetchSize";
  private static final String QUERY_IDEMPOTENCE = QUERY + '.' + "idempotence";

  private static final String SOCKET_READTIMEOUT = SOCKET + '.' + "readTimeout";
  private static final String TIMESTAMP_GENERATOR = "timestampGenerator";
  private static final String ADDRESS_TRANSLATOR = "addressTranslator";

  private static final String PROTOCOL_COMPRESSION = PROTOCOL + '.' + "compression";

  private static final String POLICY_MAX_RETRIES = POLICY + '.' + "maxRetries";
  private static final String POLICY_LBP_LOCAL_DC = POLICY + '.' + LBP + '.' + "localDc";
  private static final String POLICY_LBP_WHITE_LIST = POLICY + '.' + LBP + '.' + "whiteList";

  private final LoaderConfig config;

  private Map<DriverOption, Object> driverConfig;
  private AuthProvider authProvider;
  private SslHandlerFactory sslHandlerFactory;
  private String localDc;
  private Predicate<Node> nodeFilter;

  DriverSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init(Map<DriverOption, Object> executorConfig)
      throws GeneralSecurityException, IOException {
    try {

      driverConfig = new HashMap<>(executorConfig);

      List<String> hosts = config.getStringList(HOSTS);
      if (hosts.isEmpty()) {
        throw new BulkConfigurationException(
            "Setting driver.hosts is mandatory. Please set driver.hosts "
                + "and try again. See settings.md or help for more information.");
      }
      int port = config.getInt(PORT);
      driverConfig.put(
          CONTACT_POINTS,
          hosts.stream().map(host -> host + ':' + port).collect(Collectors.toList()));

      Compression compression = config.getEnum(Compression.class, PROTOCOL_COMPRESSION);
      if (compression != Compression.NONE) {
        driverConfig.put(
            DefaultDriverOption.PROTOCOL_COMPRESSION, compression.name().toLowerCase());
      }

      driverConfig.put(CONNECTION_POOL_LOCAL_SIZE, config.getInt(POOLING_LOCAL_CONNECTIONS));
      driverConfig.put(CONNECTION_POOL_REMOTE_SIZE, config.getInt(POOLING_REMOTE_CONNECTIONS));
      driverConfig.put(CONNECTION_MAX_REQUESTS, config.getInt(POOLING_REQUESTS));
      driverConfig.put(HEARTBEAT_INTERVAL, config.getDuration(POOLING_HEARTBEAT));
      driverConfig.put(REQUEST_CONSISTENCY, config.getString(QUERY_CONSISTENCY));
      driverConfig.put(REQUEST_SERIAL_CONSISTENCY, config.getString(QUERY_SERIALCONSISTENCY));
      driverConfig.put(REQUEST_PAGE_SIZE, config.getInt(QUERY_FETCHSIZE));
      driverConfig.put(REQUEST_DEFAULT_IDEMPOTENCE, config.getBoolean(QUERY_IDEMPOTENCE));
      driverConfig.put(REQUEST_TIMEOUT, config.getDuration(SOCKET_READTIMEOUT));

      driverConfig.put(TIMESTAMP_GENERATOR_CLASS, config.getString(TIMESTAMP_GENERATOR));
      driverConfig.put(ADDRESS_TRANSLATOR_CLASS, config.getString(ADDRESS_TRANSLATOR));

      driverConfig.put(
          LOAD_BALANCING_POLICY_CLASS, DCInferringDseLoadBalancingPolicy.class.getName());
      driverConfig.put(RETRY_POLICY_CLASS, MultipleRetryPolicy.class.getName());
      driverConfig.put(
          BulkDriverOption.RETRY_POLICY_MAX_RETRIES, config.getInt(POLICY_MAX_RETRIES));

      authProvider = AuthProviderFactory.createAuthProvider(config);
      sslHandlerFactory = SslHandlerFactoryFactory.createSslHandlerFactory(config);

      if (config.hasPath(POLICY_LBP_LOCAL_DC)) {
        localDc = config.getString(POLICY_LBP_LOCAL_DC);
      }

      List<SocketAddress> whiteList =
          config.getStringList(POLICY_LBP_WHITE_LIST).stream()
              .map(host -> new InetSocketAddress(host, port))
              .collect(Collectors.toList());
      if (!whiteList.isEmpty()) {
        nodeFilter = node -> whiteList.contains(node.getEndPoint().resolve());
      }

    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "driver");
    }
  }

  public DseSession newSession(String executionId) {
    Supplier<Config> configSupplier =
        () -> {
          ConfigFactory.invalidateCaches();
          Map<String, Object> configMap = new HashMap<>();
          for (DriverOption driverOption : driverConfig.keySet()) {
            configMap.put(driverOption.getPath(), driverConfig.get(driverOption));
          }
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(
                      ConfigFactory.parseMap(configMap, "DSBulk driver config")
                          .atPath(DEFAULT_ROOT_PATH))
                  .withFallback(ConfigFactory.defaultApplication())
                  .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig("datastax-java-driver");
        };
    DseSessionBuilder sessionBuilder =
        new DseSessionBuilder() {
          @Override
          protected DriverContext buildContext(
              DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
            return new DseDriverContext(
                configLoader, programmaticArguments, dseProgrammaticArgumentsBuilder.build()) {
              @Override
              protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
                return Optional.ofNullable(sslHandlerFactory);
              }
            };
          }
        }.withApplicationVersion(getBulkLoaderVersion())
            .withApplicationName(BULK_LOADER_APPLICATION_NAME + " " + executionId)
            .withClientId(clientId(executionId))
            .withAuthProvider(authProvider)
            .withConfigLoader(
                new DefaultDseDriverConfigLoader(configSupplier) {
                  @Override
                  public boolean supportsReloading() {
                    return false;
                  }
                });
    if (localDc != null) {
      sessionBuilder.withLocalDatacenter(localDc);
    }
    if (nodeFilter != null) {
      sessionBuilder.withNodeFilter(nodeFilter);
    }
    return sessionBuilder.build();
  }

  private enum Compression {
    NONE,
    SNAPPY,
    LZ4
  }
}
