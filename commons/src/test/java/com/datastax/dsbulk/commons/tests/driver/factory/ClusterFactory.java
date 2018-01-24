/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver.factory;

import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster;
import com.datastax.dsbulk.commons.tests.driver.annotations.ClusterConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.ClusterFactoryMethod;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** */
public abstract class ClusterFactory {

  private static final ClusterConfig DEFAULT_CLUSTER_CONFIG;

  @SuppressWarnings("unused")
  private static void dummy(@ClusterConfig Object dummy) {}

  static {
    ClusterConfig annotation = null;
    try {
      Method method = ClusterFactory.class.getDeclaredMethod("dummy", Object.class);
      annotation = (ClusterConfig) method.getParameterAnnotations()[0][0];
    } catch (NoSuchMethodException e) {
      // won't happen
    }
    DEFAULT_CLUSTER_CONFIG = annotation;
  }

  private ClusterFactory() {}

  public static ClusterFactory createInstanceForAnnotatedElement(
      AnnotatedElement element, Class<?> testClass) {
    ClusterFactoryMethod factoryRef = element.getAnnotation(ClusterFactoryMethod.class);
    ClusterConfig config = element.getAnnotation(ClusterConfig.class);
    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "Field %s can be annotated with either @ClusterFactory or @ClusterFactoryRef, but not both",
                element));
      }
      return new ClusterMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_CLUSTER_CONFIG;
    }
    return new ClusterAnnotationFactory(config);
  }

  public abstract Cluster.Builder createClusterBuilder();

  private static class ClusterAnnotationFactory extends ClusterFactory {

    private final ProtocolVersion protocolVersion;
    private final ProtocolOptions.Compression compression;
    private final boolean ssl;
    private final boolean metrics;
    private final boolean jmx;
    private final String[] credentials;
    private final Map<String, String> socketOptions;
    private final Map<String, String> poolingOptions;
    private final Map<String, String> queryOptions;
    private final Map<String, String> graphOptions;

    private ClusterAnnotationFactory(ClusterConfig config) {
      protocolVersion = config.protocolVersion().toDriverProtocolVersion();
      compression = config.compression();
      ssl = config.ssl();
      metrics = config.metrics();
      jmx = config.jmx();
      credentials = computeCredentials(config.credentials());
      socketOptions = toConfigMap(config.socketOptions());
      poolingOptions = toConfigMap(config.poolingOptions());
      queryOptions = toConfigMap(config.queryOptions());
      graphOptions = toConfigMap(config.graphOptions());
    }

    private static String[] computeCredentials(String[] credentials) {
      if (credentials.length != 0 && credentials.length != 2) {
        throw new IllegalArgumentException(
            "Credentials should be specified as an array of two elements (username and password)");
      }
      return credentials.length == 2 ? credentials : null;
    }

    private static Map<String, String> toConfigMap(String[] conf) {
      Map<String, String> config = new HashMap<>();
      for (String aConf : conf) {
        String[] tokens = aConf.split(":");
        if (tokens.length != 2) {
          throw new IllegalArgumentException("Wrong configuration option: " + aConf);
        }
        String key = tokens[0].trim();
        String value = tokens[1].trim();
        config.put(key, value);
      }
      return config;
    }

    private static void populateBean(Object bean, Map<String, String> properties) {
      try {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          String name = entry.getKey().trim();
          String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
          String[] tokens = entry.getValue().split(",");
          Method method = ReflectionUtils.locateMethod(methodName, bean.getClass(), tokens.length);
          if (method == null) {
            throw new IllegalArgumentException(
                String.format("Cannot locate method %s in class %s", methodName, bean.getClass()));
          }
          Object[] parameters = new Object[tokens.length];
          Class<?>[] types = method.getParameterTypes();
          for (int i = 0; i < tokens.length; i++) {
            parameters[i] = convert(tokens[i], types[i]);
          }
          method.invoke(bean, parameters);
        }
      } catch (Exception e) {
        throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    private static Object convert(String value, Class<?> type) {
      if (value == null) {
        return null;
      }
      value = value.trim();
      if (type.isAssignableFrom(value.getClass())) {
        return value;
      }
      if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
        return Boolean.valueOf(value);
      }
      if (type.equals(Character.class) || type.equals(Character.TYPE)) {
        return value.charAt(0);
      }
      if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
        return Integer.valueOf(value);
      }
      if (type.equals(Long.class) || type.equals(Long.TYPE)) {
        return Long.valueOf(value);
      }
      if (type.equals(Float.class) || type.equals(Float.TYPE)) {
        return Float.valueOf(value);
      }
      if (type.equals(Double.class) || type.equals(Double.TYPE)) {
        return Double.valueOf(value);
      }
      if (Enum.class.isAssignableFrom(type)) {
        return Enum.valueOf((Class<? extends Enum>) type, value);
      }
      if (type.equals(BigInteger.class)) {
        return new BigInteger(value);
      }
      if (type.equals(BigDecimal.class)) {
        return new BigDecimal(value);
      }
      return value;
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
      DseCluster.Builder clusterBuilder = DseCluster.builder();
      if (ssl) {
        clusterBuilder.withSSL(createSSLOptions());
      }
      if (!jmx) {
        clusterBuilder.withoutJMXReporting();
      }
      if (!metrics) {
        clusterBuilder.withoutMetrics();
      }
      if (credentials != null) {
        clusterBuilder.withCredentials(credentials[0], credentials[1]);
      }
      if (protocolVersion != null) {
        clusterBuilder.withProtocolVersion(protocolVersion);
      }
      SocketOptions socketOptions = new SocketOptions();
      // reduce default values
      PoolingOptions poolingOptions =
          new PoolingOptions()
              .setConnectionsPerHost(LOCAL, 1, 1)
              .setConnectionsPerHost(REMOTE, 1, 1);
      // disable event debouncing
      QueryOptions queryOptions =
          new QueryOptions()
              .setRefreshNodeIntervalMillis(0)
              .setRefreshNodeListIntervalMillis(0)
              .setRefreshSchemaIntervalMillis(0);
      GraphOptions graphOptions = new GraphOptions();
      populateBean(socketOptions, this.socketOptions);
      populateBean(poolingOptions, this.poolingOptions);
      populateBean(queryOptions, this.queryOptions);
      populateBean(graphOptions, this.graphOptions);
      clusterBuilder
          .withCompression(compression)
          .withPoolingOptions(poolingOptions)
          .withSocketOptions(socketOptions)
          .withQueryOptions(queryOptions)
          .withGraphOptions(graphOptions);
      return clusterBuilder;
    }

    private SSLOptions createSSLOptions() {
      try {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(
            this.getClass().getResourceAsStream(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PATH),
            DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray());
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        ks.load(
            this.getClass().getResourceAsStream(DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PATH),
            DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());
        KeyManagerFactory kmf =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClusterAnnotationFactory that = (ClusterAnnotationFactory) o;
      if (ssl != that.ssl) {
        return false;
      }
      if (metrics != that.metrics) {
        return false;
      }
      if (jmx != that.jmx) {
        return false;
      }
      if (protocolVersion != that.protocolVersion) {
        return false;
      }
      if (compression != that.compression) {
        return false;
      }
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      if (!Arrays.equals(credentials, that.credentials)) {
        return false;
      }
      if (!socketOptions.equals(that.socketOptions)) {
        return false;
      }
      if (!poolingOptions.equals(that.poolingOptions)) {
        return false;
      }
      if (!queryOptions.equals(that.queryOptions)) {
        return false;
      }
      return graphOptions.equals(that.graphOptions);
    }

    @Override
    public int hashCode() {
      int result = protocolVersion != null ? protocolVersion.hashCode() : 0;
      result = 31 * result + compression.hashCode();
      result = 31 * result + (ssl ? 1 : 0);
      result = 31 * result + (metrics ? 1 : 0);
      result = 31 * result + (jmx ? 1 : 0);
      result = 31 * result + Arrays.hashCode(credentials);
      result = 31 * result + socketOptions.hashCode();
      result = 31 * result + poolingOptions.hashCode();
      result = 31 * result + queryOptions.hashCode();
      result = 31 * result + graphOptions.hashCode();
      return result;
    }
  }

  private static class ClusterMethodFactory extends ClusterFactory {

    private final Method factoryMethod;

    private ClusterMethodFactory(ClusterFactoryMethod factoryRef, Class<?> testClass) {
      factoryMethod = locateClusterFactoryMethod(factoryRef, testClass);
    }

    private static Method locateClusterFactoryMethod(
        ClusterFactoryMethod factoryRef, Class<?> testClass) {
      String methodName = factoryRef.value();
      Class<?> factoryClass =
          factoryRef.factoryClass().equals(ClusterFactoryMethod.TestClass.class)
              ? testClass
              : factoryRef.factoryClass();
      Method method = ReflectionUtils.locateMethod(methodName, factoryClass, 0);
      if (method == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find factory method %s in %s", methodName, factoryClass));
      }
      if (method.getAnnotation(ClusterConfig.class) == null) {
        throw new IllegalArgumentException(
            String.format("Method %s must be annotated with @ClusterFactory", method));
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new IllegalArgumentException(String.format("Method %s must be static", method));
      }
      return method;
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
      Cluster.Builder clusterBuilder = newBuilderInstance();
      if (clusterBuilder == null) {
        throw new NullPointerException(
            String.format("Cluster factory method %s returned null", factoryMethod));
      }
      return clusterBuilder;
    }

    protected Cluster.Builder newBuilderInstance() {
      return ReflectionUtils.invokeMethod(factoryMethod, null, Cluster.Builder.class);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClusterMethodFactory that = (ClusterMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
