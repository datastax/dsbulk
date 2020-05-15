/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.tests.ccm;

import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.DSE;
import static java.util.concurrent.TimeUnit.SECONDS;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.datastax.oss.dsbulk.commons.utils.PlatformUtils;
import com.datastax.oss.dsbulk.tests.RemoteClusterExtension;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.ccm.factory.CCMClusterFactory;
import com.datastax.oss.dsbulk.tests.logging.LogUtils;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.Version;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A manager for {@link CCMCluster CCM} clusters that helps testing with JUnit 5 and Cassandra. */
public class CCMExtension extends RemoteClusterExtension
    implements ExecutionCondition, TestExecutionExceptionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CCMExtension.class);

  private static final String CCM = "CCM";

  private volatile RuntimeException createError;

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (DefaultCCMCluster.CCM_TYPE == DSE && PlatformUtils.isWindows()) {
      return ConditionEvaluationResult.disabled(
          "CCM tests are configured to use DSE which is not compatible with Windows");
    }
    Class<?> testClass = context.getRequiredTestClass();
    CCMRequirements requirements =
        ReflectionUtils.locateClassAnnotation(testClass, CCMRequirements.class);
    if (requirements != null) {
      if (!Arrays.asList(requirements.compatibleTypes()).contains(DefaultCCMCluster.CCM_TYPE)) {
        return ConditionEvaluationResult.disabled(
            String.format(
                "Test is not compatible with CCM cluster type in use: %s.",
                DefaultCCMCluster.CCM_TYPE));
      }
      for (CCMVersionRequirement requirement : requirements.versionRequirements()) {
        if (requirement.type() == DefaultCCMCluster.CCM_TYPE) {
          Version min = Version.parse(requirement.min());
          Version max = Version.parse(requirement.max());
          if (!Version.isWithinRange(min, max, DefaultCCMCluster.CCM_VERSION)) {
            return ConditionEvaluationResult.disabled(
                String.format(
                    "Test requires version in range [%s,%s[ but %s is configured.",
                    requirement.min(), requirement.max(), DefaultCCMCluster.CCM_VERSION));
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
  public void handleTestExecutionException(ExtensionContext context, Throwable throwable)
      throws Throwable {
    if (shouldPrintDiagnostic(throwable)) {
      LOGGER.error("CCM test failed due to a server failure or timeout", throwable);
      CCMCluster ccm = getOrCreateCCM(context);
      ccm.printDiagnostics();
    }
    throw throwable;
  }

  private static boolean shouldPrintDiagnostic(Throwable throwable) {
    if (isServerFailureOrTimeout(throwable)) {
      return true;
    }
    Throwable cause = throwable.getCause();
    if (cause != null) {
      return isServerFailureOrTimeout(cause);
    }
    return false;
  }

  private static boolean isServerFailureOrTimeout(Throwable throwable) {
    return throwable instanceof CCMException
        || throwable instanceof ServerError
        || throwable instanceof OverloadedException
        || throwable instanceof DriverTimeoutException;
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
              if (createError != null) {
                throw createError;
              }
              int attempts = 1;
              while (true) {
                CCMClusterFactory factory =
                    CCMClusterFactory.createInstanceForClass(context.getRequiredTestClass());
                DefaultCCMCluster ccm = null;
                try {
                  ccm = factory.createCCMClusterBuilder().build();
                  ccm.start();
                  return ccm;
                } catch (RuntimeException e) {
                  if (attempts == 3) {
                    createError = e;
                    LOGGER.error("Could not start CCM cluster, giving up", e);
                    throw e;
                  }
                  if (ccm != null) {
                    try {
                      ccm.stop();
                      ccm.remove();
                    } catch (Exception ignored) {
                    }
                  }
                  LOGGER.error("Could not start CCM cluster, retrying");
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
      try {
        LogUtils.resetLogbackConfiguration();
      } catch (JoranException ignored) {
      }
    }
  }
}
