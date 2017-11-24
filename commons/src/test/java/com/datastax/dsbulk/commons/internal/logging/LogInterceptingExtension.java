/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.lang.reflect.Parameter;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class LogInterceptingExtension
    implements ParameterResolver, BeforeTestExecutionCallback, AfterTestExecutionCallback {

  private static final Namespace LOG_INTERCEPTOR = Namespace.create(LogInterceptingExtension.class);

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    return parameter.getType().equals(LogInterceptor.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    DefaultLogInterceptor interceptor =
        extensionContext
            .getStore(LOG_INTERCEPTOR)
            .getOrComputeIfAbsent(
                extensionContext.getRequiredTestClass(),
                method -> {
                  LogCapture annotation =
                      parameterContext.getParameter().getAnnotation(LogCapture.class);
                  if (annotation != null) {
                    String loggerName =
                        annotation.value().equals(LogCapture.Root.class)
                            ? ROOT_LOGGER_NAME
                            : annotation.value().getName();
                    int level = annotation.level().toInt();
                    return new DefaultLogInterceptor(loggerName, level);
                  }
                  return new DefaultLogInterceptor();
                },
                DefaultLogInterceptor.class);
    interceptor.start();
    return interceptor;
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) throws Exception {
    DefaultLogInterceptor interceptor =
        context
            .getStore(LOG_INTERCEPTOR)
            .get(context.getRequiredTestClass(), DefaultLogInterceptor.class);
    if (interceptor != null) {
      interceptor.start();
    }
  }

  @Override
  public void afterTestExecution(ExtensionContext context) throws Exception {
    DefaultLogInterceptor interceptor =
        context
            .getStore(LOG_INTERCEPTOR)
            .get(context.getRequiredTestClass(), DefaultLogInterceptor.class);
    if (interceptor != null) {
      interceptor.stop();
    }
  }
}
