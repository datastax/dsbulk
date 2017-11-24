/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.tests.driver.factory.ClusterFactory;
import com.datastax.dsbulk.tests.driver.factory.SessionFactory;
import java.io.Closeable;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
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

/** */
public abstract class RemoteClusterExtension implements AfterAllCallback, ParameterResolver {

  public static final ExtensionContext.Namespace TEST_NAMESPACE =
      ExtensionContext.Namespace.create("com.datastax.dsbulk.tests");

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteClusterExtension.class);

  private static final String CLOSEABLES_KEY = "CLOSEABLES";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(DseCluster.class)
        || type.isAssignableFrom(DseSession.class)
        || type.isAssignableFrom(ContinuousPagingSession.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    Closeable value;
    if (type.isAssignableFrom(DseSession.class)
        || type.isAssignableFrom(ContinuousPagingSession.class)) {
      value = createSession(parameter, extensionContext);
      registerCloseable(value, extensionContext);
    } else if (type.isAssignableFrom(DseCluster.class)) {
      value = createCluster(parameter, extensionContext);
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

  private Session createSession(Parameter parameter, ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    SessionFactory sessionFactory =
        SessionFactory.createInstanceForAnnotatedElement(parameter, testClass);
    return createSession(sessionFactory, context);
  }

  private Session createSession(SessionFactory sessionFactory, ExtensionContext context) {
    Cluster cluster = createCluster(sessionFactory.getClusterFactory(), context);
    Session session = cluster.connect();
    sessionFactory.configureSession(session);
    return session;
  }

  private Cluster createCluster(Parameter parameter, ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    ClusterFactory config = ClusterFactory.createInstanceForAnnotatedElement(parameter, testClass);
    return createCluster(config, context);
  }

  private Cluster createCluster(ClusterFactory config, ExtensionContext context) {
    Cluster.Builder clusterBuilder = config.createClusterBuilder();
    // use a different codec registry for each cluster instance
    clusterBuilder.withCodecRegistry(new CodecRegistry());
    // add contact points only if the provided builder didn't do so
    if (clusterBuilder.getContactPoints().isEmpty()) {
      clusterBuilder.addContactPoints(getContactPoints(context));
    }
    clusterBuilder.withPort(getBinaryPort(context));
    return clusterBuilder.build();
  }

  protected abstract int getBinaryPort(ExtensionContext context);

  protected abstract List<InetAddress> getContactPoints(ExtensionContext context);

  private void registerCloseable(AutoCloseable value, ExtensionContext context) {
    ExtensionContext.Store store = context.getStore(TEST_NAMESPACE);
    @SuppressWarnings("unchecked")
    Set<AutoCloseable> closeables =
        store.getOrComputeIfAbsent(CLOSEABLES_KEY, clazz -> new HashSet<>(), Set.class);
    closeables.add(value);
  }

  private void closeCloseables(ExtensionContext context) throws Exception {
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
      if (value instanceof Session) {
        ((Session) value).getCluster().close();
      } else {
        value.close();
      }
    } catch (Exception e) {
      LOGGER.error("Could not close " + value, e);
    }
  }
}
