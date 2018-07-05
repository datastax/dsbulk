/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.ccm;

import static com.datastax.dsbulk.commons.tests.utils.Version.DEFAULT_DSE_VERSION;
import static com.datastax.dsbulk.commons.tests.utils.Version.DEFAULT_OSS_VERSION;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.RemoteClusterExtension;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.factory.CCMClusterFactory;
import com.datastax.dsbulk.commons.tests.utils.NetworkUtils;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.dsbulk.commons.tests.utils.VersionRequirement;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A manager for {@link CCMCluster CCM} clusters that helps testing with JUnit 5 and Cassandra. */
public class CCMExtension extends RemoteClusterExtension implements ExecutionCondition {

  private static final Logger LOGGER = LoggerFactory.getLogger(CCMExtension.class);
  private static final String CCM = "CCM";

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    CCMConfig config = ReflectionUtils.locateClassAnnotation(testClass, CCMConfig.class);
    VersionRequirement requirement =
        ReflectionUtils.locateClassAnnotation(testClass, VersionRequirement.class);
    if (config != null) {
      if (config.dse() && PlatformUtils.isWindows()) {
        return ConditionEvaluationResult.disabled("Test not compatible with windows");
      }
    }
    if (requirement != null) {
      Version min = Version.parse(requirement.min());
      Version max = Version.parse(requirement.max());
      Version def = config == null || config.dse() ? DEFAULT_DSE_VERSION : DEFAULT_OSS_VERSION;
      if (!Version.isWithinRange(min, max, def)) {
        return ConditionEvaluationResult.disabled(
            String.format(
                "Test requires version in range [%s,%s[ but %s is configured.",
                requirement.min(), requirement.max(), def));
      }
    }
    return ConditionEvaluationResult.enabled("OK");
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(DefaultCCMCluster.class)
        || super.supportsParameter(parameterContext, extensionContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type.isAssignableFrom(DefaultCCMCluster.class)) {
      CCMCluster ccm = getOrCreateCCM(extensionContext);
      LOGGER.debug(String.format("Returning %s for parameter %s", ccm, parameter));
      return ccm;
    } else {
      return super.resolveParameter(parameterContext, extensionContext);
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    super.afterAll(context);
    stopCCM(context);
  }

  @Override
  protected int getBinaryPort(ExtensionContext context) {
    return getOrCreateCCM(context).getBinaryPort();
  }

  @Override
  protected List<InetSocketAddress> getContactPoints(ExtensionContext context) {
    int binaryPort = getBinaryPort(context);
    return getOrCreateCCM(context)
        .getInitialContactPoints()
        .stream()
        .map(addr -> new InetSocketAddress(addr, binaryPort))
        .collect(Collectors.toList());
  }

  private CCMCluster getOrCreateCCM(ExtensionContext context) {
    return context
        .getStore(TEST_NAMESPACE)
        .getOrComputeIfAbsent(
            CCM,
            f -> {
              String liveClusterConfig = System.getProperty("liveCluster");
              if (liveClusterConfig != null) {
                return new LiveCCMCluster(liveClusterConfig);
              } else {
                CCMClusterFactory factory =
                    CCMClusterFactory.createInstanceForClass(context.getRequiredTestClass());
                return factory.createCCMClusterBuilder().build();
              }
            },
            CCMCluster.class);
  }

  private void stopCCM(ExtensionContext context) {
    CCMCluster ccm = context.getStore(TEST_NAMESPACE).remove(CCM, CCMCluster.class);
    if (ccm != null) {
      ccm.stop();
    }
  }

  private static class LiveCCMCluster implements CCMCluster {
    private final Config config;
    private final List<InetAddress> contactPoints;
    private final int[] nodesPerDC;
    private final String ipPrefix;
    private final Version version;

    LiveCCMCluster(String config) {
      this.config =
          ConfigFactory.parseString(config)
              .withFallback(
                  ConfigFactory.parseString(
                      "port=9042, contactPoints=[127.0.0.1], clusterName=preexisting, nodesPerDc=[1], dse=true"));

      //
      contactPoints =
          this.config
              .getList("contactPoints")
              .unwrapped()
              .stream()
              .map(
                  addr -> {
                    try {
                      return InetAddress.getByName((String) addr);
                    } catch (UnknownHostException e) {
                      e.printStackTrace();
                    }
                    return null;
                  })
              .collect(Collectors.toList());

      //
      List<Object> nodesPerDCList = this.config.getList("nodesPerDc").unwrapped();
      nodesPerDC = new int[nodesPerDCList.size()];
      for (int idx = 0; idx < nodesPerDCList.size(); idx++) {
        nodesPerDC[idx] = (int) nodesPerDCList.get(idx);
      }

      //
      String firstPoint = (String) this.config.getList("contactPoints").get(0).unwrapped();
      List<String> parts = Splitter.on(".").splitToList(firstPoint);
      ipPrefix = String.format("%s.%s.%s.", parts.get(0), parts.get(1), parts.get(2));

      //
      if (this.config.hasPath("version")) {
        version = Version.parse(this.config.getString("version"));
      } else if (isDSE()) {
        version = Version.DEFAULT_DSE_VERSION;
      } else {
        version = Version.DEFAULT_OSS_VERSION;
      }
    }

    @Override
    public String getClusterName() {
      return config.getString("clusterName");
    }

    @Override
    public String getIpPrefix() {
      return ipPrefix;
    }

    @Override
    public int getBinaryPort() {
      return config.getInt("port");
    }

    @Override
    public List<InetAddress> getInitialContactPoints() {
      return contactPoints;
    }

    @Override
    public InetSocketAddress addressOfNode(int node) {
      return new InetSocketAddress(NetworkUtils.addressOfNode(ipPrefix, node), getBinaryPort());
    }

    @Override
    public InetSocketAddress addressOfNode(int dc, int node) {
      return new InetSocketAddress(
          NetworkUtils.addressOfNode(ipPrefix, nodesPerDC, dc, node), getBinaryPort());
    }

    @Override
    public void start() {
      // no-op
    }

    @Override
    public void stop() {
      // no-op
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public void start(int node) {
      // no-op
    }

    @Override
    public void stop(int node) {
      // no-op
    }

    @Override
    public void startDC(int dc) {
      // no-op
    }

    @Override
    public void stopDC(int dc) {
      // no-op
    }

    @Override
    public void start(int dc, int node) {
      // no-op
    }

    @Override
    public void stop(int dc, int node) {
      // no-op
    }

    @Override
    public void waitForUp(int node) {
      // no-op
    }

    @Override
    public void waitForDown(int node) {
      // no-op
    }

    @Override
    public void waitForUp(int dc, int node) {
      // no-op
    }

    @Override
    public void waitForDown(int dc, int node) {
      // no-op
    }

    @Override
    public Version getVersion() {
      return version;
    }

    @Override
    public boolean isDSE() {
      return config.getBoolean("dse");
    }

    @Override
    public File getCcmDir() {
      return null;
    }

    @Override
    public File getClusterDir() {
      return null;
    }

    @Override
    public File getNodeDir(int node) {
      return null;
    }

    @Override
    public File getNodeConfDir(int node) {
      return null;
    }

    @Override
    public int getStoragePort() {
      return 7000;
    }

    @Override
    public int getThriftPort() {
      return 9160;
    }

    @Override
    public void setKeepLogs() {
      // no-op
    }

    @Override
    public void forceStop() {
      // no-op
    }

    @Override
    public void remove() {
      // no-op
    }

    @Override
    public void updateConfig(Map<String, Object> configs) {
      // no-op
    }

    @Override
    public void updateDSEConfig(Map<String, Object> configs) {
      // no-op
    }

    @Override
    public String checkForErrors() {
      return null;
    }

    @Override
    public void forceStop(int node) {
      // no-op
    }

    @Override
    public void remove(int node) {
      // no-op
    }

    @Override
    public void add(int node) {
      // no-op
    }

    @Override
    public void add(int dc, int node) {
      // no-op
    }

    @Override
    public void decommission(int node) {
      // no-op
    }

    @Override
    public void updateNodeConfig(int node, String key, Object value) {
      // no-op
    }

    @Override
    public void updateNodeConfig(int node, Map<String, Object> configs) {
      // no-op
    }

    @Override
    public void updateDSENodeConfig(int node, String key, Object value) {
      // no-op
    }

    @Override
    public void updateDSENodeConfig(int node, Map<String, Object> configs) {
      // no-op
    }

    @Override
    public void setWorkload(int node, Workload... workload) {
      // no-op
    }
  }
}
