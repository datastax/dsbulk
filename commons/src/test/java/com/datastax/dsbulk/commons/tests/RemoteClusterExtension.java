/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests;

import com.datastax.dsbulk.commons.tests.driver.factory.SessionFactory;
import com.datastax.dse.driver.api.core.DseSession;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RemoteClusterExtension implements AfterAllCallback, ParameterResolver {

  protected static final ExtensionContext.Namespace TEST_NAMESPACE =
      ExtensionContext.Namespace.create("com.datastax.dsbulk.commons.tests");

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteClusterExtension.class);

  private static final String CLOSEABLES_KEY = "CLOSEABLES";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(DseSession.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    AutoCloseable value;
    if (type.isAssignableFrom(DseSession.class)) {
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

  private DseSession createSession(Parameter parameter, ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    SessionFactory sessionFactory =
        SessionFactory.createInstanceForAnnotatedElement(
            parameter, testClass, getLocalDCName(context));
    return createSession(sessionFactory, context);
  }

  private DseSession createSession(SessionFactory sessionFactory, ExtensionContext context) {
    List<InetSocketAddress> contactPoints =
        getContactPoints(context)
            .stream()
            .map(inetAddr -> new InetSocketAddress(inetAddr, getBinaryPort(context)))
            .collect(Collectors.toList());
    DseSession session =
        sessionFactory.createSessionBuilder().addContactPoints(contactPoints).build();
    sessionFactory.configureSession(session);
    return session;
  }

  protected abstract String getLocalDCName(ExtensionContext context);

  protected abstract int getBinaryPort(ExtensionContext context);

  protected abstract List<InetAddress> getContactPoints(ExtensionContext context);

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
