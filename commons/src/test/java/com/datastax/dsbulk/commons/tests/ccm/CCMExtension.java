/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.CCM_TYPE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.CCM_VERSION;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.RemoteClusterExtension;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.commons.tests.ccm.factory.CCMClusterFactory;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A manager for {@link CCMCluster CCM} clusters that helps testing with JUnit 5 and Cassandra. */
public class CCMExtension extends RemoteClusterExtension implements ExecutionCondition {

  private static final Logger LOGGER = LoggerFactory.getLogger(CCMExtension.class);

  private static final String CCM = "CCM";

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (CCM_TYPE == DSE && PlatformUtils.isWindows()) {
      return ConditionEvaluationResult.disabled(
          "CCM tests are configured to use DSE which is not compatible with Windows");
    }
    Class<?> testClass = context.getRequiredTestClass();
    CCMRequirements requirements =
        ReflectionUtils.locateClassAnnotation(testClass, CCMRequirements.class);
    if (requirements != null) {
      if (!Arrays.asList(requirements.compatibleTypes()).contains(CCM_TYPE)) {
        return ConditionEvaluationResult.disabled(
            String.format("Test is not compatible with CCM cluster type in use: %s.", CCM_TYPE));
      }
      for (CCMVersionRequirement requirement : requirements.versionRequirements()) {
        if (requirement.type() == CCM_TYPE) {
          Version min = Version.parse(requirement.min());
          Version max = Version.parse(requirement.max());
          if (!Version.isWithinRange(min, max, CCM_VERSION)) {
            return ConditionEvaluationResult.disabled(
                String.format(
                    "Test requires version in range [%s,%s[ but %s is configured.",
                    requirement.min(), requirement.max(), CCM_VERSION));
          }
        }
      }
    }
    return ConditionEvaluationResult.enabled("OK");
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(DefaultCCMCluster.class)
        || super.supportsParameter(parameterContext, extensionContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type.isAssignableFrom(DefaultCCMCluster.class)) {
      CCMCluster ccm = getOrCreateCCM(extensionContext);
      LOGGER.debug(String.format("Returning %s for parameter %s", ccm, parameter));
      return ccm;
    } else {
      return super.resolveParameter(parameterContext, extensionContext);
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    super.afterAll(context);
    stopCCM(context);
  }

  @Override
  protected List<EndPoint> getContactPoints(ExtensionContext context) {
    return getOrCreateCCM(context).getInitialContactPoints();
  }

  @Override
  protected String getLocalDatacenter(ExtensionContext context) {
    CCMCluster ccm = getOrCreateCCM(context);
    try {
      return ccm.getDC(1);
    } catch (Exception e) {
      LOGGER.warn("Could not determine local DC name, using default names instead");
      return ccm.isMultiDC() ? "dc1" : "Cassandra";
    }
  }

  private CCMCluster getOrCreateCCM(ExtensionContext context) {
    return context
        .getStore(TEST_NAMESPACE)
        .getOrComputeIfAbsent(
            CCM,
            f -> {
              int attempts = 1;
              while (true) {
                try {
                  CCMClusterFactory factory =
                      CCMClusterFactory.createInstanceForClass(context.getRequiredTestClass());
                  DefaultCCMCluster ccm = factory.createCCMClusterBuilder().build();
                  ccm.start();
                  return ccm;
                } catch (Exception e) {
                  if (attempts == 3) {
                    LOGGER.error("Could not start CCM cluster, giving up", e);
                    throw e;
                  }
                  LOGGER.error("Could not start CCM cluster, retrying", e);
                  Uninterruptibles.sleepUninterruptibly(10, SECONDS);
                }
                attempts++;
              }
            },
            CCMCluster.class);
  }

  private void stopCCM(ExtensionContext context) {
    CCMCluster ccm = context.getStore(TEST_NAMESPACE).remove(CCM, CCMCluster.class);
    if (ccm != null) {
      ccm.stop();
    }
  }
}
