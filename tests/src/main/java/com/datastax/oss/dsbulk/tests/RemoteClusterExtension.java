/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.dsbulk.tests.driver.factory.SessionFactory;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RemoteClusterExtension implements AfterAllCallback, ParameterResolver {

  protected static final ExtensionContext.Namespace TEST_NAMESPACE =
      ExtensionContext.Namespace.create("com.datastax.oss.dsbulk.tests");

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteClusterExtension.class);

  private static final String CLOSEABLES_KEY = "CLOSEABLES";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(CqlSession.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    AutoCloseable value;
    if (type.isAssignableFrom(CqlSession.class)) {
      value = createSession(parameter, extensionContext);
      registerCloseable(value, extensionContext);
    } else {
      throw new ParameterResolutionException("Cannot resolve parameter " + parameter);
    }
    return value;
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    closeCloseables(context);
  }

  protected CqlSession createSession(Parameter parameter, ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    SessionFactory sessionFactory =
        SessionFactory.createInstanceForAnnotatedElement(
            parameter, testClass, getLocalDatacenter(context));
    return createSession(sessionFactory, context);
  }

  protected CqlSession createSession(SessionFactory sessionFactory, ExtensionContext context) {
    List<EndPoint> contactPoints = getContactPoints(context);
    CqlSession session =
        sessionFactory.createSessionBuilder().addContactEndPoints(contactPoints).build();
    sessionFactory.configureSession(session);
    return session;
  }

  protected abstract String getLocalDatacenter(ExtensionContext context);

  protected abstract List<EndPoint> getContactPoints(ExtensionContext context);

  private void registerCloseable(AutoCloseable value, ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(TEST_NAMESPACE);
    @SuppressWarnings("unchecked")
    Set<AutoCloseable> closeables =
        store.getOrComputeIfAbsent(CLOSEABLES_KEY, clazz -> new HashSet<>(), Set.class);
    closeables.add(value);
  }

  private void closeCloseables(ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(TEST_NAMESPACE);
    @SuppressWarnings("unchecked")
    Set<AutoCloseable> closeables = store.remove(CLOSEABLES_KEY, Set.class);
    if (closeables != null) {
      for (AutoCloseable c : closeables) {
        closeQuietly(c);
      }
    }
  }

  private void closeQuietly(AutoCloseable value) {
    try {
      value.close();
    } catch (Exception e) {
      LOGGER.error("Could not close " + value, e);
    }
  }
}
