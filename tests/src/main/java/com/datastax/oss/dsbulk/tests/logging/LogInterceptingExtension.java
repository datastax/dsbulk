/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import java.lang.reflect.Parameter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.event.Level;

public class LogInterceptingExtension
    implements ParameterResolver,
        BeforeTestExecutionCallback,
        BeforeEachCallback,
        AfterTestExecutionCallback,
        AfterEachCallback {

  private static final Namespace LOG_INTERCEPTOR = Namespace.create(LogInterceptingExtension.class);

  private static final String INTERCEPTORS = "INTERCEPTORS";

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
    LogCapture annotation = parameterContext.getParameter().getAnnotation(LogCapture.class);
    @SuppressWarnings("unchecked")
    ConcurrentMap<LogInterceptorKey, DefaultLogInterceptor> interceptors =
        extensionContext
            .getStore(LOG_INTERCEPTOR)
            .getOrComputeIfAbsent(
                INTERCEPTORS,
                k -> new ConcurrentHashMap<StreamType, DefaultLogInterceptor>(),
                ConcurrentMap.class);
    return interceptors.computeIfAbsent(
        new LogInterceptorKey(annotation),
        key -> {
          DefaultLogInterceptor interceptor =
              new DefaultLogInterceptor(key.loggerName, key.level.toInt());
          interceptor.start();
          return interceptor;
        });
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultLogInterceptor> interceptors =
        context.getStore(LOG_INTERCEPTOR).get(INTERCEPTORS, ConcurrentMap.class);
    if (interceptors != null) {
      interceptors.values().forEach(DefaultLogInterceptor::start);
    }
  }

  @Override
  public void afterTestExecution(ExtensionContext context) throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultLogInterceptor> interceptors =
        context.getStore(LOG_INTERCEPTOR).get(INTERCEPTORS, ConcurrentMap.class);
    if (interceptors != null) {
      interceptors.values().forEach(DefaultLogInterceptor::stop);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    LogResource annotation =
        ReflectionUtils.locateClassAnnotation(context.getRequiredTestClass(), LogResource.class);
    if (annotation != null) {
      LogUtils.resetLogbackConfiguration(annotation.value());
    } else {
      LogUtils.resetLogbackConfiguration();
    }
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultLogInterceptor> interceptors =
        context.getStore(LOG_INTERCEPTOR).get(INTERCEPTORS, ConcurrentMap.class);
    if (interceptors != null) {
      interceptors.values().forEach(DefaultLogInterceptor::stop);
      interceptors.values().forEach(DefaultLogInterceptor::start);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    LogUtils.resetLogbackConfiguration();
  }

  private static class LogInterceptorKey {

    private final String loggerName;
    private final Level level;

    private LogInterceptorKey(LogCapture annotation) {
      this.loggerName =
          annotation == null || annotation.value().equals(LogCapture.Root.class)
              ? ROOT_LOGGER_NAME
              : annotation.value().getName();
      this.level = annotation == null ? Level.INFO : annotation.level();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LogInterceptorKey)) {
        return false;
      }
      LogInterceptorKey that = (LogInterceptorKey) o;
      return loggerName.equals(that.loggerName) && level == that.level;
    }

    @Override
    public int hashCode() {
      int result = loggerName.hashCode();
      result = 31 * result + level.hashCode();
      return result;
    }
  }
}
