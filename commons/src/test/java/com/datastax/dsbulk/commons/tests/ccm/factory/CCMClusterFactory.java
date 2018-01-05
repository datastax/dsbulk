/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests.ccm.factory;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMFactory;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMFactoryMethod;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
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
  private static final Map<String, Version> CONFIG_VERSION_REQUIREMENTS;

  static {
    Map<String, Version> map = new HashMap<>(1);
    map.put("enable_user_defined_functions", Version.parse("2.2.0"));
    CONFIG_VERSION_REQUIREMENTS = Collections.unmodifiableMap(map);
  }

  private CCMClusterFactory() {}

  public static CCMClusterFactory createInstanceForClass(Class<?> testClass) {
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
      return new CCMClusterMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_CCM_CONFIG;
    }
    return new CCMClusterAnnotationFactory(config);
  }

  public abstract DefaultCCMCluster.Builder createCCMClusterBuilder();

  private static class CCMClusterAnnotationFactory extends CCMClusterFactory {

    private final int[] numberOfNodes;
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
      this.ssl = config.ssl();
      this.auth = config.auth();
      this.cassandraConfig = toConfigMap(config.config());
      this.dseConfig = toConfigMap(config.dseConfig());
      this.jvmArgs = toConfigSet(config.jvmArgs());
      this.createOptions = toConfigSet(config.createOptions());
      this.workloads = computeWorkloads(config);
    }

    private static Map<String, Object> toConfigMap(String[] conf) {
      Map<String, Object> config = new HashMap<>();
      for (String aConf : conf) {
        String[] tokens = aConf.split(":");
        if (tokens.length != 2) {
          throw new IllegalArgumentException("Wrong configuration option: " + aConf);
        }
        String key = tokens[0];
        String value = tokens[1];
        // If we've detected a property with a version requirement, skip it if the version requirement
        // cannot be met.
        if (CONFIG_VERSION_REQUIREMENTS.containsKey(key)) {
          Version requirement = CONFIG_VERSION_REQUIREMENTS.get(key);
          if (Version.DEFAULT_OSS_VERSION.compareTo(requirement) < 0) {
            LOGGER.debug(
                "Skipping inclusion of '{}' in cassandra.yaml since it requires >= C* {} and {} "
                    + "was detected.",
                aConf,
                requirement,
                Version.DEFAULT_OSS_VERSION);
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
      for (int perDc : config.numberOfNodes()) {
        total += perDc;
      }
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
      if (dse) {
        ccmBuilder.withDSE();
      }
      if (ssl) {
        ccmBuilder.withSSL();
      }
      if (auth) {
        ccmBuilder.withAuth();
      }
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
        if (workload != null) {
          ccmBuilder.withWorkload(i + 1, workload);
        }
      }
      return ccmBuilder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CCMClusterAnnotationFactory that = (CCMClusterAnnotationFactory) o;
      return dse == that.dse
          && ssl == that.ssl
          && auth == that.auth
          && Arrays.equals(numberOfNodes, that.numberOfNodes)
          && cassandraConfig.equals(that.cassandraConfig)
          && dseConfig.equals(that.dseConfig)
          && jvmArgs.equals(that.jvmArgs)
          && createOptions.equals(that.createOptions)
          && workloads.equals(that.workloads);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(numberOfNodes);
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

  private static class CCMClusterMethodFactory extends CCMClusterFactory {

    private final Method factoryMethod;

    private CCMClusterMethodFactory(CCMFactoryMethod factoryRef, Class<?> testClass) {
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
      if (ccmBuilder == null) {
        throw new NullPointerException(
            String.format("CCM factory method %s returned null", factoryMethod));
      }
      return ccmBuilder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CCMClusterMethodFactory that = (CCMClusterMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
