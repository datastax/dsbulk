/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.DSERequirement;
import com.datastax.dsbulk.tests.ccm.factory.CCMClusterFactory;
import com.datastax.dsbulk.tests.ccm.factory.ClusterFactory;
import com.datastax.dsbulk.tests.ccm.factory.SessionFactory;
import com.datastax.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.tests.utils.VersionUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.Closer;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A manager for {@link CCMCluster CCM} clusters that helps testing with JUnit 4 and Cassandra.
 *
 * <p>This manager is a JUnit {@link org.junit.rules.TestRule rule} that must be declared in each
 * test class requiring CCM, like in the example below:
 *
 * <pre>
 * &#64;Rule &#64;ClassRule
 * public static CCMRule ccmRule = new CCMRule();
 * </pre>
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
    DSERequirement dseRequirement = description.getAnnotation(DSERequirement.class);
    CCMConfig config = ReflectionUtils.locateClassAnnotation(testClass, CCMConfig.class);
    CassandraVersion currentVersion =
        CassandraVersion.parse(VersionUtils.computeVersion(config, true));
    if (dseRequirement != null) {
      // if the configured DSE DSERequirement exceeds the one being used skip this test.
      if (PlatformUtils.isWindows()) {
        return new Statement() {

          @Override
          public void evaluate() throws Throwable {
            throw new AssumptionViolatedException("Test not compatible with windows");
          }
        };
      }

      if (!dseRequirement.min().isEmpty()) {
        CassandraVersion minVersion = CassandraVersion.parse(dseRequirement.min());
        if (minVersion.compareTo(currentVersion) > 0) {
          // Create a statement which simply indicates that the configured DSE DSERequirement is too old for this test.
          return new Statement() {

            @Override
            public void evaluate() throws Throwable {
              throw new AssumptionViolatedException(
                  "Test requires C* "
                      + minVersion
                      + " but "
                      + currentVersion
                      + " is configured.  Description: "
                      + dseRequirement.description());
            }
          };
        }
      }

      if (!dseRequirement.max().isEmpty()) {
        // if the test version exceeds the maximum configured one, fail out.
        CassandraVersion maxVersion = CassandraVersion.parse(dseRequirement.max());

        if (maxVersion.compareTo(currentVersion) <= 0) {
          return new Statement() {

            @Override
            public void evaluate() throws Throwable {
              throw new AssumptionViolatedException(
                  "Test requires C* less than "
                      + maxVersion
                      + " but "
                      + currentVersion
                      + " is configured.  Description: "
                      + dseRequirement.description());
            }
          };
        }
      }
    }

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

  private void getOrCreateCCMCluster() throws ExecutionException {
    CCMClusterFactory config = CCMClusterFactory.createInstanceForClass(testClass);
    DefaultCCMCluster.Builder ccmBuilder = config.createCCMClusterBuilder().notStarted();
    ccm = CACHE.get(ccmBuilder);
    ccm.start();
    LOGGER.debug("Using {}", ccm);
  }

  private void createAndInjectCloseables() throws IllegalAccessException {
    Set<Field> fields = ReflectionUtils.locateFieldsAnnotatedWith(testClass, Inject.class);
    for (Field field : fields) {
      field.setAccessible(true);
      assert Modifier.isStatic(field.getModifiers())
          : String.format("Field %s is not static", field);
      Class<?> type = field.getType();
      assert Closeable.class.isAssignableFrom(type)
          : String.format("Field %s is not Closeable", field);
      Closeable value;
      if (CCMCluster.class.isAssignableFrom(type)) {
        value = ccm;
      } else if (Session.class.isAssignableFrom(type)) {
        Session session = getSession(field, testClass);
        closer.register(session.getCluster());
        value = session;
      } else if (Cluster.class.isAssignableFrom(type)) {
        value = getCluster(field, testClass);
        closer.register(value);
      } else {
        throw new IllegalStateException("Cannot inject field " + field);
      }
      LOGGER.debug(String.format("Injecting %s into field %s", value, field));
      try {
        field.set(null, value);
      } catch (IllegalAccessException e) {
        LOGGER.error("Error while injecting field %s" + field, e);
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
