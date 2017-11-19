/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.tests.RemoteClusterExtension;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.factory.CCMClusterFactory;
import com.datastax.dsbulk.tests.utils.ReflectionUtils;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
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
    Class<?> testClass = context.getRequiredTestClass();
    CCMConfig config = ReflectionUtils.locateClassAnnotation(testClass, CCMConfig.class);
    if (config != null) {
      if (config.dse() && PlatformUtils.isWindows()) {
        return ConditionEvaluationResult.disabled("Test not compatible with windows");
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
      LOGGER.warn(String.format("Returning %s for parameter %s", ccm, parameter));
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
  protected int getBinaryPort(ExtensionContext context) {
    return getOrCreateCCM(context).getBinaryPort();
  }

  @Override
  protected List<InetAddress> getContactPoints(ExtensionContext context) {
    return getOrCreateCCM(context).getInitialContactPoints();
  }

  private CCMCluster getOrCreateCCM(ExtensionContext context) {
    return context
        .getStore(TEST_NAMESPACE)
        .getOrComputeIfAbsent(
            CCM,
            f -> {
              CCMClusterFactory factory =
                  CCMClusterFactory.createInstanceForClass(context.getRequiredTestClass());
              DefaultCCMCluster ccm = factory.createCCMClusterBuilder().build();
              ccm.start();
              return ccm;
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
