/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver.factory;

import static com.datastax.dsbulk.commons.tests.utils.SessionUtils.createSimpleKeyspace;
import static com.datastax.dsbulk.commons.tests.utils.SessionUtils.useKeyspace;

import com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionFactoryMethod;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.testinfra.session.TestConfigLoader;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;

@SuppressWarnings("SimplifiableIfStatement")
public abstract class SessionFactory {

  private static final SessionConfig DEFAULT_SESSION_CONFIG;

  @SuppressWarnings("unused")
  private static void dummy(@SessionConfig Object dummy) {}

  static {
    SessionConfig annotation = null;
    try {
      Method method = SessionFactory.class.getDeclaredMethod("dummy", Object.class);
      annotation = (SessionConfig) method.getParameterAnnotations()[0][0];
    } catch (NoSuchMethodException e) {
      // won't happen
    }
    DEFAULT_SESSION_CONFIG = annotation;
  }

  public static SessionFactory createInstanceForAnnotatedElement(
      AnnotatedElement element, Class<?> testClass) {
    SessionFactoryMethod factoryRef = element.getAnnotation(SessionFactoryMethod.class);
    SessionConfig config = element.getAnnotation(SessionConfig.class);
    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "Field %s can be annotated with either @SessionFactory or @SessionFactoryRef, but not both",
                element));
      }
      return new SessionMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_SESSION_CONFIG;
    }
    return new SessionAnnotationFactory(config);
  }

  public abstract DseSessionBuilder createSessionBuilder();

  public void configureSession(CqlSession session) {
    // nothing to do by default
  }

  private static class SessionAnnotationFactory extends SessionFactory {

    private static final String[] SSL_OPTIONS = {
      "advanced.ssl-engine-factory.class = DefaultSslEngineFactory",
      "advanced.ssl-engine-factory.truststore-path = "
          + DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE,
      "advanced.ssl-engine-factory.truststore-password = "
          + DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD,
      "advanced.ssl-engine-factory.keystore-path = "
          + DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE,
      "advanced.ssl-engine-factory.keystore-password = "
          + DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD,
    };

    private final SessionConfig.UseKeyspaceMode useKeyspaceMode;
    private final String loggedKeyspaceName;
    private final TestConfigLoader configLoader;

    private SessionAnnotationFactory(SessionConfig config) {
      useKeyspaceMode = config.useKeyspace();
      loggedKeyspaceName = config.loggedKeyspaceName();
      String[] settings =
          new String[config.settings().length + (config.ssl() ? SSL_OPTIONS.length : 0) + 1];
      settings[0] = "basic.load-balancing-policy.local-datacenter=\"Cassandra\"";
      int curIdx = 1;
      for (String opt : config.settings()) {
        settings[curIdx++] = opt;
      }
      if (config.ssl()) {
        for (String opt : SSL_OPTIONS) {
          settings[curIdx++] = opt;
        }
      }
      configLoader = new TestConfigLoader(settings);
    }

    @Override
    public void configureSession(CqlSession session) {
      String keyspace;
      switch (useKeyspaceMode) {
        case NONE:
          return;
        case GENERATE:
          keyspace = StringUtils.uniqueIdentifier("ks");
          break;
        case FIXED:
        default:
          keyspace = loggedKeyspaceName;
          break;
      }
      createSimpleKeyspace(session, keyspace);
      useKeyspace(session, keyspace);
    }

    @Override
    public DseSessionBuilder createSessionBuilder() {
      return DseSession.builder().withConfigLoader(configLoader);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionAnnotationFactory that = (SessionAnnotationFactory) o;
      return useKeyspaceMode == that.useKeyspaceMode
          && Objects.equals(loggedKeyspaceName, that.loggedKeyspaceName)
          && Objects.equals(configLoader.getInitialConfig(), that.configLoader.getInitialConfig());
    }

    @Override
    public int hashCode() {
      return Objects.hash(useKeyspaceMode, loggedKeyspaceName, configLoader.getInitialConfig());
    }
  }

  private static class SessionMethodFactory extends SessionFactory {

    private final Method factoryMethod;

    private SessionMethodFactory(SessionFactoryMethod factoryRef, Class<?> testClass) {
      factoryMethod = locateSessionFactoryMethod(factoryRef, testClass);
    }

    private static Method locateSessionFactoryMethod(
        SessionFactoryMethod factoryRef, Class<?> testClass) {
      String methodName = factoryRef.value();
      Class<?> factoryClass =
          factoryRef.factoryClass().equals(SessionFactoryMethod.TestClass.class)
              ? testClass
              : factoryRef.factoryClass();
      Method method = ReflectionUtils.locateMethod(methodName, factoryClass, 0);
      if (method == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find factory method %s in %s", methodName, factoryClass));
      }
      if (method.getAnnotation(SessionConfig.class) == null) {
        throw new IllegalArgumentException(
            String.format("Method %s must be annotated with @SessionFactory", method));
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new IllegalArgumentException(String.format("Method %s must be static", method));
      }
      return method;
    }

    @Override
    public DseSessionBuilder createSessionBuilder() {
      DseSessionBuilder sessionBuilder = newBuilderInstance();
      if (sessionBuilder == null) {
        throw new NullPointerException(
            String.format("Session factory method %s returned null", factoryMethod));
      }
      return sessionBuilder;
    }

    protected DseSessionBuilder newBuilderInstance() {
      return ReflectionUtils.invokeMethod(factoryMethod, null, DseSessionBuilder.class);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionFactory.SessionMethodFactory that = (SessionFactory.SessionMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
