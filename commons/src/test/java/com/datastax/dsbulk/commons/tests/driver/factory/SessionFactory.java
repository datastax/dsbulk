/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver.factory;

import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.utils.SessionUtils.createSimpleKeyspace;
import static com.datastax.dsbulk.commons.tests.utils.SessionUtils.useKeyspace;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_USER_NAME;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.HEARTBEAT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE;
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
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.List;
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

  public abstract CqlSessionBuilder createSessionBuilder();

  public void configureSession(CqlSession session) {
    // nothing to do by default
  }

  private static class SessionAnnotationFactory extends SessionFactory {

    private static final ImmutableMap<DriverOption, String> SSL_OPTIONS =
        ImmutableMap.<DriverOption, String>builder()
            .put(SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory")
            .put(SSL_TRUSTSTORE_PATH, DEFAULT_CLIENT_TRUSTSTORE_FILE.toString())
            .put(SSL_TRUSTSTORE_PASSWORD, DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    private static final ImmutableMap<DriverOption, String> AUTH_OPTIONS =
        ImmutableMap.<DriverOption, String>builder()
            .put(SSL_KEYSTORE_PATH, DEFAULT_CLIENT_KEYSTORE_FILE.toString())
            .put(SSL_KEYSTORE_PASSWORD, DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .build();

    private static final Duration ONE_MINUTE = Duration.ofSeconds(60);

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
      ProgrammaticDriverConfigLoaderBuilder loaderBuilder =
          DseDriverConfigLoader.programmaticBuilder()
              .withInt(CONNECTION_POOL_LOCAL_SIZE, 1)
              .withInt(CONNECTION_POOL_REMOTE_SIZE, 1)
              .withString(LOAD_BALANCING_LOCAL_DATACENTER, dcName)
              .withBoolean(REQUEST_WARN_IF_SET_KEYSPACE, false)
              // set most timeouts to ridiculously high values
              .withDuration(REQUEST_TIMEOUT, ONE_MINUTE)
              .withDuration(CONNECTION_INIT_QUERY_TIMEOUT, ONE_MINUTE)
              .withDuration(CONNECTION_SET_KEYSPACE_TIMEOUT, ONE_MINUTE)
              .withDuration(METADATA_SCHEMA_REQUEST_TIMEOUT, ONE_MINUTE)
              .withDuration(HEARTBEAT_TIMEOUT, ONE_MINUTE)
              .withDuration(CONTROL_CONNECTION_TIMEOUT, ONE_MINUTE)
              .withDuration(CONTROL_CONNECTION_AGREEMENT_TIMEOUT, ONE_MINUTE)
              // speed up tests with aggressive Netty values
              .withInt(NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0)
              .withInt(NETTY_IO_SHUTDOWN_TIMEOUT, 15)
              .withString(NETTY_IO_SHUTDOWN_UNIT, "SECONDS")
              .withInt(NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0)
              .withInt(NETTY_ADMIN_SHUTDOWN_TIMEOUT, 15)
              .withString(NETTY_ADMIN_SHUTDOWN_UNIT, "SECONDS");

      Config settings = ConfigFactory.empty();
      for (String opt : config.settings()) {
        settings = ConfigFactory.parseString(opt).withFallback(settings);
      }
      settings
          .entrySet()
          .forEach(
              entry ->
                  setOption(
                      loaderBuilder, optionFor(entry.getKey()), entry.getValue().unwrapped()));
      String[] credentials = computeCredentials(config.credentials());
      if (credentials != null) {
        loaderBuilder
            .withClass(AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
            .withString(AUTH_PROVIDER_USER_NAME, credentials[0])
            .withString(AUTH_PROVIDER_PASSWORD, credentials[1]);
      }
      if (config.auth()) {
        for (Map.Entry<DriverOption, String> entry : AUTH_OPTIONS.entrySet()) {
          setOption(loaderBuilder, entry.getKey(), entry.getValue());
        }
      }
      if (config.ssl() || config.auth()) {
        for (Map.Entry<DriverOption, String> entry : SSL_OPTIONS.entrySet()) {
          setOption(loaderBuilder, entry.getKey(), entry.getValue());
        }
        setOption(loaderBuilder, SSL_HOSTNAME_VALIDATION, config.hostnameVerification());
      }
      configLoader = loaderBuilder.build();
    }

    private DriverOption optionFor(String key) {
      for (DefaultDriverOption option : DefaultDriverOption.values()) {
        if (option.getPath().equals(key)) {
          return option;
        }
      }
      for (DseDriverOption option : DseDriverOption.values()) {
        if (option.getPath().equals(key)) {
          return option;
        }
      }
      throw new IllegalArgumentException("Unknown option: " + key);
    }

    @SuppressWarnings("unchecked")
    private void setOption(
        ProgrammaticDriverConfigLoaderBuilder loaderBuilder, DriverOption option, Object value) {
      if (value == null) {
        loaderBuilder.without(option);
      } else {
        if (value instanceof Boolean) {
          loaderBuilder.withBoolean(option, (Boolean) value);
        } else if (value instanceof Integer) {
          loaderBuilder.withInt(option, (Integer) value);
        } else if (value instanceof Long) {
          loaderBuilder.withLong(option, (Long) value);
        } else if (value instanceof Double) {
          loaderBuilder.withDouble(option, (Double) value);
        } else if (value instanceof String) {
          loaderBuilder.withString(option, (String) value);
        } else if (value instanceof Class) {
          loaderBuilder.withClass(option, (Class) value);
        } else if (value instanceof Duration) {
          loaderBuilder.withDuration(option, (Duration) value);
        } else if (value instanceof List) {
          // FIXME this is a hack to avoid inspecting the contents of the list
          loaderBuilder.withBooleanList(option, (List) value);
        } else if (value instanceof Map) {
          loaderBuilder.withStringMap(option, (Map) value);
        } else {
          throw new IllegalArgumentException("Unknown option type: " + value.getClass());
        }
      }
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
    public CqlSessionBuilder createSessionBuilder() {
      return CqlSession.builder().withConfigLoader(configLoader);
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
    public CqlSessionBuilder createSessionBuilder() {
      CqlSessionBuilder sessionBuilder = newBuilderInstance();
      if (sessionBuilder == null) {
        throw new NullPointerException(
            String.format("Session factory method %s returned null", factoryMethod));
      }
      return sessionBuilder;
    }

    CqlSessionBuilder newBuilderInstance() {
      return ReflectionUtils.invokeMethod(factoryMethod, null, CqlSessionBuilder.class);
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
