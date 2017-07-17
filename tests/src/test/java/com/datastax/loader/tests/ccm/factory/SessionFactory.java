/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.ccm.factory;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseSession;
import com.datastax.loader.tests.ccm.annotations.SessionConfig;
import com.datastax.loader.tests.utils.SessionUtils;
import com.datastax.loader.tests.utils.StringUtils;
import java.lang.reflect.Field;

/** */
@SuppressWarnings("SimplifiableIfStatement")
public class SessionFactory {

  private static final SessionConfig DEFAULT_SESSION_CONFIG;

  @SuppressWarnings("unused")
  @SessionConfig
  private static Object dummy;

  static {
    SessionConfig annotation = null;
    try {
      Field field = SessionFactory.class.getDeclaredField("dummy");
      annotation = field.getAnnotation(SessionConfig.class);
    } catch (NoSuchFieldException e) {
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

  public static SessionFactory createInstanceForField(Field field, Class<?> testClass) {
    ClusterFactory clusterFactory = ClusterFactory.createInstanceForField(field, testClass);
    SessionConfig ann = field.getAnnotation(SessionConfig.class);
    if (ann == null) ann = DEFAULT_SESSION_CONFIG;
    boolean dse =
        DseSession.class.isAssignableFrom(field.getType())
            || ContinuousPagingSession.class.isAssignableFrom(field.getType());
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
        keyspace = StringUtils.uniqueIdentifier();
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SessionFactory that = (SessionFactory) o;
    if (useKeyspaceMode != that.useKeyspaceMode) return false;
    return loggedKeyspaceName.equals(that.loggedKeyspaceName);
  }

  @Override
  public int hashCode() {
    int result = useKeyspaceMode.hashCode();
    result = 31 * result + loggedKeyspaceName.hashCode();
    return result;
  }
}
