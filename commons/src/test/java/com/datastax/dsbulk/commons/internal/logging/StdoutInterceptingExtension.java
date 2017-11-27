/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import java.lang.reflect.Parameter;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class StdoutInterceptingExtension implements ParameterResolver, AfterTestExecutionCallback {

  private static final Namespace STDOUT_INTERCEPTOR =
      Namespace.create(StdoutInterceptingExtension.class);

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    return parameter.getType().equals(StdoutInterceptor.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    DefaultStdoutInterceptor interceptor =
        extensionContext
            .getStore(STDOUT_INTERCEPTOR)
            .getOrComputeIfAbsent(
                extensionContext.getRequiredTestClass(),
                method -> new DefaultStdoutInterceptor(),
                DefaultStdoutInterceptor.class);
    interceptor.start();
    return interceptor;
  }

  @Override
  public void afterTestExecution(ExtensionContext context) throws Exception {
    DefaultStdoutInterceptor interceptor =
        context
            .getStore(STDOUT_INTERCEPTOR)
            .get(context.getRequiredTestClass(), DefaultStdoutInterceptor.class);
    if (interceptor != null) {
      interceptor.stop();
    }
  }
}
