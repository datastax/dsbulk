/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.factory.CCMClusterFactory;
import com.datastax.dsbulk.tests.ccm.factory.ClusterFactory;
import com.datastax.dsbulk.tests.ccm.factory.SessionFactory;
import com.datastax.dsbulk.tests.utils.ReflectionUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.Closer;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A manager for {@link CCMCluster CCM} clusters that helps testing with JUnit 4 and Cassandra.
 *
 * <p>
 *
 * <p>
 *
 * <p>
 *
 * <p>This manager is a JUnit {@link org.junit.rules.TestRule rule} that must be declared in each
 * test class requiring CCM, like in the example below:
 *
 * <p>
 *
 * <p>
 *
 * <p>
 *
 * <pre>
 * &#64;Rule &#64;ClassRule
 * public static CCMRule ccmRule = new CCMRule();
 * </pre>
 *
 * <p>
 *
 * <p>Note that the field MUST be public, static, and annotated with {@link
 * org.junit.ClassRule @ClassRule}. Each test class must also be annotated with {@link
 * CCMTest @CCMTest}.
 */
public class CCMRule implements TestRule {

  private static final Logger LOGGER = LoggerFactory.getLogger(CCMRule.class);

  private static final String TEST_CLASS_NAME_KEY = "com.datastax.dsbulk.tests.TEST_CLASS_NAME";

  private static final LoadingCache<DefaultCCMCluster.Builder, DefaultCCMCluster> CACHE =
      CacheBuilder.newBuilder()
          .initialCapacity(1)
          .maximumSize(1)
          .softValues()
          .removalListener(
              (RemovalListener<DefaultCCMCluster.Builder, DefaultCCMCluster>)
                  notification -> {
                    DefaultCCMCluster ccm = notification.getValue();
                    if (ccm != null) {
                      try {
                        ccm.close();
                      } catch (Exception e) {
                        LOGGER.error("Error closing remote cluster " + ccm, e);
                      }
                    }
                  })
          .build(
              new CacheLoader<DefaultCCMCluster.Builder, DefaultCCMCluster>() {
                @Override
                public DefaultCCMCluster load(DefaultCCMCluster.Builder key) throws Exception {
                  return key.build();
                }
              });

  private final Closer closer = Closer.create();
  private Class<?> testClass;
  private CCMCluster ccm;

  private static void putTestContext(Description description) {
    MDC.put(TEST_CLASS_NAME_KEY, description.getTestClass().getSimpleName());
  }

  private static void removeTestContext() {
    MDC.remove(TEST_CLASS_NAME_KEY);
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    testClass = description.getTestClass();
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        putTestContext(description);
        try {
          getOrCreateCCMCluster();
          createAndInjectCloseables();
          base.evaluate();
        } catch (Throwable t) {
          LOGGER.error("Error while initializing remote cluster", t);
          throw t;
        } finally {
          closeCloseables();
          removeTestContext();
        }
      }
    };
  }

  private void getOrCreateCCMCluster() throws ExecutionException, IllegalAccessException {
    CCMClusterFactory config = CCMClusterFactory.createInstanceForClass(testClass);
    DefaultCCMCluster.Builder ccmBuilder = config.createCCMClusterBuilder();
    ccm = CACHE.get(ccmBuilder);
    ccm.start();
    LOGGER.debug("Using {}", ccm);
  }

  private void createAndInjectCloseables() throws IllegalAccessException {
    Set<Field> fields = ReflectionUtils.locateFieldsAnnotatedWith(testClass, Inject.class);
    for (Field field : fields) {
      try {
        Closeable value = null;
        Class<?> type = field.getType();
        if (CCMCluster.class.isAssignableFrom(type)) {
          value = ccm;
        } else if (Session.class.isAssignableFrom(type)) {
          Session session = getSession(field, testClass);
          closer.register(session.getCluster());
          value = session;
        } else if (Cluster.class.isAssignableFrom(type)) {
          value = getCluster(field, testClass);
          closer.register(value);
        }
        if (value != null) {
          field.setAccessible(true);
          field.set(null, value);
        }
      } catch (IllegalAccessException e) {
        LOGGER.error(
            String.format("Error while injecting field %s in instance %s", field, null), e);
        throw e;
      }
    }
  }

  private void closeCloseables() throws IOException {
    closer.close();
  }

  private Cluster getCluster(Field field, Class<?> testClass) {
    ClusterFactory config = ClusterFactory.createInstanceForField(field, testClass);
    return createCluster(config);
  }

  private Session getSession(Field field, Class<?> testClass) {
    SessionFactory sessionFactory = SessionFactory.createInstanceForField(field, testClass);
    return createSession(sessionFactory);
  }

  private Cluster createCluster(ClusterFactory config) {
    Cluster.Builder clusterBuilder = config.createClusterBuilder();
    // use a different codec registry for each cluster instance
    clusterBuilder.withCodecRegistry(new CodecRegistry());
    // add contact points only if the provided builder didn't do so
    if (clusterBuilder.getContactPoints().isEmpty())
      clusterBuilder.addContactPoints(ccm.getInitialContactPoints());
    clusterBuilder.withPort(ccm.getBinaryPort());
    return clusterBuilder.build();
  }

  private Session createSession(SessionFactory sessionFactory) {
    Cluster cluster = createCluster(sessionFactory.getClusterFactory());
    Session session = cluster.connect();
    sessionFactory.configureSession(session);
    return session;
  }
}
