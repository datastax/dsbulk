/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm.factory;

import com.datastax.driver.core.VersionNumber;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.DefaultCCMCluster;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMFactory;
import com.datastax.dsbulk.tests.ccm.annotations.CCMFactoryMethod;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.tests.utils.VersionUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public abstract class CCMClusterFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CCMClusterFactory.class);

  @CCMConfig
  private static class Dummy {}

  private static final CCMConfig DEFAULT_CCM_CONFIG = Dummy.class.getAnnotation(CCMConfig.class);

  // A mapping of cassandra.yaml factory options to their version requirements.
  // If a factory compute passed containing one of these options and the version requirement cannot be met
  // the option compute simply filtered.
  private static final Map<String, VersionNumber> CONFIG_VERSION_REQUIREMENTS;

  static {
    Map<String, VersionNumber> map = new HashMap<>(1);
    map.put("enable_user_defined_functions", VersionNumber.parse("2.2.0"));
    CONFIG_VERSION_REQUIREMENTS = Collections.unmodifiableMap(map);
  }

  private CCMClusterFactory() {}

  public static CCMClusterFactory createInstanceForClass(Class<?> testClass) {
    CCMTest ann = testClass.getAnnotation(CCMTest.class);
    if (ann == null)
      throw new IllegalArgumentException(
          String.format("%s is not annotated with @CCMTest", testClass));
    CCMFactoryMethod factoryRef =
        ReflectionUtils.locateClassAnnotation(testClass, CCMFactoryMethod.class);
    CCMConfig config = ReflectionUtils.locateClassAnnotation(testClass, CCMConfig.class);
    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "%s can be annotated with either @CCMClusterFactory or @CCMFactoryRef, but not both",
                testClass));
      }
      return new CCMCLusterMethodFactory(factoryRef, testClass);
    }
    if (config == null) config = DEFAULT_CCM_CONFIG;
    return new CCMClusterAnnotationFactory(config);
  }

  public abstract DefaultCCMCluster.Builder createCCMClusterBuilder();

  private static class CCMClusterAnnotationFactory extends CCMClusterFactory {

    private final int[] numberOfNodes;
    private final String version;
    private final boolean dse;
    private final boolean ssl;
    private final boolean auth;
    private final Map<String, Object> cassandraConfig;
    private final Map<String, Object> dseConfig;
    private final Set<String> jvmArgs;
    private final Set<String> createOptions;
    private final List<CCMCluster.Workload[]> workloads;

    private CCMClusterAnnotationFactory(CCMConfig config) {
      this.numberOfNodes = config.numberOfNodes();
      this.dse = config.dse();
      this.version = computeVersion(config, dse);
      this.ssl = config.ssl();
      this.auth = config.auth();
      this.cassandraConfig = toConfigMap(version, config.config());
      this.dseConfig = toConfigMap(version, config.dseConfig());
      this.jvmArgs = toConfigSet(config.jvmArgs());
      this.createOptions = toConfigSet(config.createOptions());
      this.workloads = computeWorkloads(config);
    }

    private static String computeVersion(CCMConfig config, boolean dse) {
      return VersionUtils.getBestVersion(config.version(), dse);
    }

    private static Map<String, Object> toConfigMap(String version, String[] conf) {
      Map<String, Object> config = new HashMap<>();
      for (String aConf : conf) {
        String[] tokens = aConf.split(":");
        if (tokens.length != 2)
          throw new IllegalArgumentException("Wrong configuration option: " + aConf);
        String key = tokens[0];
        String value = tokens[1];
        // If we've detected a property with a version requirement, skip it if the version requirement
        // cannot be met.
        if (CONFIG_VERSION_REQUIREMENTS.containsKey(key)) {
          VersionNumber requirement = CONFIG_VERSION_REQUIREMENTS.get(key);
          if (VersionNumber.parse(version).compareTo(requirement) < 0) {
            LOGGER.debug(
                "Skipping inclusion of '{}' in cassandra.yaml since it requires >= C* {} and {} "
                    + "was detected.",
                aConf,
                requirement,
                version);
            continue;
          }
        }
        config.put(key, value);
      }
      return config;
    }

    private static Set<String> toConfigSet(String[] config) {
      Set<String> args = new LinkedHashSet<>();
      Collections.addAll(args, config);
      return args;
    }

    private static List<CCMCluster.Workload[]> computeWorkloads(CCMConfig config) {
      int total = 0;
      for (int perDc : config.numberOfNodes()) total += perDc;
      List<CCMCluster.Workload[]> workloads =
          new ArrayList<>(Collections.<CCMCluster.Workload[]>nCopies(total, null));
      CCMWorkload[] annWorkloads = config.workloads();
      for (int i = 0; i < annWorkloads.length; i++) {
        CCMWorkload nodeWorkloads = annWorkloads[i];
        workloads.set(i, nodeWorkloads.value());
      }
      return workloads;
    }

    @Override
    public DefaultCCMCluster.Builder createCCMClusterBuilder() {
      DefaultCCMCluster.Builder ccmBuilder = DefaultCCMCluster.builder().withNodes(numberOfNodes);
      if (version != null) ccmBuilder.withVersion(version);
      if (dse) ccmBuilder.withDSE();
      if (ssl) ccmBuilder.withSSL();
      if (auth) ccmBuilder.withAuth();
      for (Map.Entry<String, Object> entry : cassandraConfig.entrySet()) {
        ccmBuilder.withCassandraConfiguration(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, Object> entry : dseConfig.entrySet()) {
        ccmBuilder.withDSEConfiguration(entry.getKey(), entry.getValue());
      }
      for (String option : createOptions) {
        ccmBuilder.withCreateOptions(option);
      }
      for (String arg : jvmArgs) {
        ccmBuilder.withJvmArgs(arg);
      }
      for (int i = 0; i < workloads.size(); i++) {
        CCMCluster.Workload[] workload = workloads.get(i);
        if (workload != null) ccmBuilder.withWorkload(i + 1, workload);
      }
      return ccmBuilder;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CCMClusterAnnotationFactory that = (CCMClusterAnnotationFactory) o;
      if (dse != that.dse) return false;
      if (ssl != that.ssl) return false;
      if (auth != that.auth) return false;
      if (!Arrays.equals(numberOfNodes, that.numberOfNodes)) return false;
      if (!version.equals(that.version)) return false;
      if (!cassandraConfig.equals(that.cassandraConfig)) return false;
      if (!dseConfig.equals(that.dseConfig)) return false;
      if (!jvmArgs.equals(that.jvmArgs)) return false;
      if (!createOptions.equals(that.createOptions)) return false;
      return workloads.equals(that.workloads);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(numberOfNodes);
      result = 31 * result + version.hashCode();
      result = 31 * result + (dse ? 1 : 0);
      result = 31 * result + (ssl ? 1 : 0);
      result = 31 * result + (auth ? 1 : 0);
      result = 31 * result + cassandraConfig.hashCode();
      result = 31 * result + dseConfig.hashCode();
      result = 31 * result + jvmArgs.hashCode();
      result = 31 * result + createOptions.hashCode();
      result = 31 * result + workloads.hashCode();
      return result;
    }
  }

  private static class CCMCLusterMethodFactory extends CCMClusterFactory {

    private final Method factoryMethod;

    private CCMCLusterMethodFactory(CCMFactoryMethod factoryRef, Class<?> testClass) {
      factoryMethod = locateCCMFactoryMethod(factoryRef, testClass);
    }

    private static Method locateCCMFactoryMethod(CCMFactoryMethod factoryRef, Class<?> testClass) {
      String methodName = factoryRef.value();
      Class<?> factoryClass =
          factoryRef.factoryClass().equals(CCMFactoryMethod.TestClass.class)
              ? testClass
              : factoryRef.factoryClass();
      Method method = ReflectionUtils.locateMethod(methodName, factoryClass, 0);
      if (method == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find factory method %s in %s", methodName, factoryClass));
      }
      if (method.getAnnotation(CCMFactory.class) == null) {
        throw new IllegalArgumentException(
            String.format("Method %s must be annotated with @CCMFactory", method));
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new IllegalArgumentException(String.format("Method %s must be static", method));
      }
      return method;
    }

    @Override
    public DefaultCCMCluster.Builder createCCMClusterBuilder() {
      DefaultCCMCluster.Builder ccmBuilder =
          ReflectionUtils.invokeMethod(factoryMethod, null, DefaultCCMCluster.Builder.class);
      if (ccmBuilder == null)
        throw new NullPointerException(
            String.format("CCM factory method %s returned null", factoryMethod));
      return ccmBuilder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CCMCLusterMethodFactory that = (CCMCLusterMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
