/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests.simulacron.factory;

import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronFactory;
import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronFactoryMethod;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** */
public abstract class BoundClusterFactory {

  @SimulacronConfig
  private static class Dummy {}

  private static final SimulacronConfig DEFAULT_SIMULACRON_CONFIG =
      BoundClusterFactory.Dummy.class.getAnnotation(SimulacronConfig.class);

  private BoundClusterFactory() {}

  public static BoundClusterFactory createInstanceForClass(Class<?> testClass) {
    SimulacronFactoryMethod factoryRef =
        ReflectionUtils.locateClassAnnotation(testClass, SimulacronFactoryMethod.class);
    SimulacronConfig config =
        ReflectionUtils.locateClassAnnotation(testClass, SimulacronConfig.class);
    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "%s can be annotated with either @SimulacronClusterFactory or @SimulacronFactoryRef, but not both",
                testClass));
      }
      return new SimulacronClusterMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_SIMULACRON_CONFIG;
    }
    return new SimulacronClusterAnnotationFactory(config);
  }

  public abstract ClusterSpec createClusterSpec();

  private static class SimulacronClusterAnnotationFactory extends BoundClusterFactory {

    private final int[] numberOfNodes;
    private final int numberOfTokens;
    private final String version;
    private final boolean dse;
    private final Map<String, Object> peerInfo;

    private SimulacronClusterAnnotationFactory(SimulacronConfig config) {
      this.numberOfNodes = config.numberOfNodes();
      this.numberOfTokens = config.numberOfTokens();
      this.version = config.version();
      this.dse = config.dse();
      this.peerInfo = toConfigMap(config.peerInfo());
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
        config.put(key, value);
      }
      return config;
    }

    @Override
    public ClusterSpec createClusterSpec() {
      ClusterSpec.Builder builder =
          ClusterSpec.builder().withNodes(numberOfNodes).withNumberOfTokens(numberOfTokens);
      if (dse) {
        if (version.isEmpty()) {
          builder.withDSEVersion(Version.DEFAULT_DSE_VERSION.toString());
        } else {
          builder.withDSEVersion(version);
        }
      } else {
        if (version.isEmpty()) {
          builder.withCassandraVersion(Version.DEFAULT_OSS_VERSION.toString());
        } else {
          builder.withCassandraVersion(version);
        }
      }
      if (!peerInfo.isEmpty()) {
        builder.withPeerInfo(peerInfo);
      }
      return builder.build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimulacronClusterAnnotationFactory that = (SimulacronClusterAnnotationFactory) o;
      return numberOfTokens == that.numberOfTokens
          && dse == that.dse
          && Arrays.equals(numberOfNodes, that.numberOfNodes)
          && version.equals(that.version)
          && peerInfo.equals(that.peerInfo);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(numberOfNodes);
      result = 31 * result + numberOfTokens;
      result = 31 * result + version.hashCode();
      result = 31 * result + (dse ? 1 : 0);
      result = 31 * result + peerInfo.hashCode();
      return result;
    }
  }

  private static class SimulacronClusterMethodFactory extends BoundClusterFactory {

    private final Method factoryMethod;

    private SimulacronClusterMethodFactory(SimulacronFactoryMethod factoryRef, Class<?> testClass) {
      factoryMethod = locateSimulacronFactoryMethod(factoryRef, testClass);
    }

    private static Method locateSimulacronFactoryMethod(
        SimulacronFactoryMethod factoryRef, Class<?> testClass) {
      String methodName = factoryRef.value();
      Class<?> factoryClass =
          factoryRef.factoryClass().equals(SimulacronFactoryMethod.TestClass.class)
              ? testClass
              : factoryRef.factoryClass();
      Method method = ReflectionUtils.locateMethod(methodName, factoryClass, 0);
      if (method == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find factory method %s in %s", methodName, factoryClass));
      }
      if (method.getAnnotation(SimulacronFactory.class) == null) {
        throw new IllegalArgumentException(
            String.format("Method %s must be annotated with @SimulacronFactory", method));
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new IllegalArgumentException(String.format("Method %s must be static", method));
      }
      return method;
    }

    @Override
    public ClusterSpec createClusterSpec() {
      ClusterSpec clusterSpec =
          ReflectionUtils.invokeMethod(factoryMethod, null, ClusterSpec.class);
      if (clusterSpec == null) {
        throw new NullPointerException(
            String.format("Simulacron factory method %s returned null", factoryMethod));
      }
      return clusterSpec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimulacronClusterMethodFactory that = (SimulacronClusterMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
