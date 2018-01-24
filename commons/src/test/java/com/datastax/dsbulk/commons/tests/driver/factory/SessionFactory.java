/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver.factory;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.utils.SessionUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;

/** */
@SuppressWarnings("SimplifiableIfStatement")
public class SessionFactory {

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

  private final ClusterFactory clusterFactory;
  private final SessionConfig.UseKeyspaceMode useKeyspaceMode;
  private final String loggedKeyspaceName;

  private SessionFactory(ClusterFactory clusterFactory, SessionConfig config) {
    this.clusterFactory = clusterFactory;
    this.useKeyspaceMode = config.useKeyspace();
    this.loggedKeyspaceName = config.loggedKeyspaceName();
  }

  public static SessionFactory createInstanceForAnnotatedElement(
      AnnotatedElement element, Class<?> testClass) {
    ClusterFactory clusterFactory =
        ClusterFactory.createInstanceForAnnotatedElement(element, testClass);
    SessionConfig ann = element.getAnnotation(SessionConfig.class);
    if (ann == null) {
      ann = DEFAULT_SESSION_CONFIG;
    }
    return new SessionFactory(clusterFactory, ann);
  }

  public ClusterFactory getClusterFactory() {
    return clusterFactory;
  }

  public void configureSession(Session session) {
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
    SessionUtils.createSimpleKeyspace(session, keyspace);
    SessionUtils.useKeyspace(session, keyspace);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SessionFactory that = (SessionFactory) o;
    if (useKeyspaceMode != that.useKeyspaceMode) {
      return false;
    }
    return loggedKeyspaceName.equals(that.loggedKeyspaceName);
  }

  @Override
  public int hashCode() {
    int result = useKeyspaceMode.hashCode();
    result = 31 * result + loggedKeyspaceName.hashCode();
    return result;
  }
}
