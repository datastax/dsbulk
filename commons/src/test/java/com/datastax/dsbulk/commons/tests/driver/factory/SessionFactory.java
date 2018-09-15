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
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_USER_NAME;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_KEYSTORE_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_KEYSTORE_PATH;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_TRUSTSTORE_PATH;

import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionFactoryMethod;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Map;
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
      AnnotatedElement element, Class<?> testClass, String dcName) {
    SessionFactoryMethod factoryRef = element.getAnnotation(SessionFactoryMethod.class);
    SessionConfig config = element.getAnnotation(SessionConfig.class);

    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "Field %s can be annotated with either @SessionConfig or @SessionFactoryMethod, but not both",
                element));
      }
      return new SessionMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_SESSION_CONFIG;
    }
    return new SessionAnnotationFactory(config, dcName);
  }

  public abstract DseSessionBuilder createSessionBuilder();

  public void configureSession(CqlSession session) {
    // nothing to do by default
  }

  private static class SessionAnnotationFactory extends SessionFactory {

    private static final ImmutableMap<DriverOption, String> SSL_OPTIONS =
        ImmutableMap.<DriverOption, String>builder()
            .put(SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory")
            .put(SSL_TRUSTSTORE_PATH, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.toString())
            .put(SSL_TRUSTSTORE_PASSWORD, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .put(SSL_KEYSTORE_PATH, CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.toString())
            .put(SSL_KEYSTORE_PASSWORD, CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .put(SSL_HOSTNAME_VALIDATION, "false")
            .build();

    private final SessionConfig.UseKeyspaceMode useKeyspaceMode;
    private final String loggedKeyspaceName;
    private final DriverConfigLoader configLoader;

    private static String[] computeCredentials(String[] credentials) {
      if (credentials.length != 0 && credentials.length != 2) {
        throw new IllegalArgumentException(
            "Credentials should be specified as an array of two elements (username and password)");
      }
      return credentials.length == 2 ? credentials : null;
    }

    private SessionAnnotationFactory(SessionConfig config, String dcName) {
      useKeyspaceMode = config.useKeyspace();
      loggedKeyspaceName = config.loggedKeyspaceName();
      // init-query-timeout is 500ms by default, which sometimes triggers in CI builds.
      DefaultDriverConfigLoaderBuilder loaderBuilder =
          SessionUtils.configLoaderBuilder()
              .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, dcName)
              .withDuration(
                  DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(3));
      for (String opt : config.settings()) {
        Config keyAndVal = ConfigFactory.parseString(opt);
        keyAndVal
            .entrySet()
            .forEach(entry -> loaderBuilder.with(entry.getKey(), entry.getValue().unwrapped()));
      }

      String[] credentials = computeCredentials(config.credentials());
      if (credentials != null) {
        loaderBuilder
            .withClass(AUTH_PROVIDER_CLASS, DsePlainTextAuthProvider.class)
            .withString(AUTH_PROVIDER_USER_NAME, credentials[0])
            .withString(AUTH_PROVIDER_PASSWORD, credentials[1]);
      }

      if (config.ssl()) {
        for (Map.Entry<DriverOption, String> entry : SSL_OPTIONS.entrySet()) {
          loaderBuilder.with(entry.getKey(), entry.getValue());
        }
      }
      configLoader = loaderBuilder.build();
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
      if (method.getAnnotation(
              com.datastax.dsbulk.commons.tests.driver.annotations.SessionFactory.class)
          == null) {
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

    DseSessionBuilder newBuilderInstance() {
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
