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
package com.datastax.oss.dsbulk.tests.logging;

import java.lang.reflect.Parameter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class StreamInterceptingExtension
    implements ParameterResolver, BeforeTestExecutionCallback, AfterTestExecutionCallback {

  private static final Namespace STREAM_INTERCEPTOR =
      Namespace.create(StreamInterceptingExtension.class);

  private static final String INTERCEPTORS = "INTERCEPTORS";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    return parameter.getType().equals(StreamInterceptor.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    StreamCapture annotation = parameterContext.getParameter().getAnnotation(StreamCapture.class);
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultStreamInterceptor> interceptors =
        extensionContext
            .getStore(STREAM_INTERCEPTOR)
            .getOrComputeIfAbsent(
                INTERCEPTORS,
                k -> new ConcurrentHashMap<StreamType, DefaultStreamInterceptor>(),
                ConcurrentMap.class);
    return interceptors.computeIfAbsent(
        annotation == null ? StreamType.STDOUT : annotation.value(),
        streamType -> {
          DefaultStreamInterceptor interceptor = new DefaultStreamInterceptor(streamType);
          interceptor.start();
          return interceptor;
        });
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultStreamInterceptor> interceptors =
        context.getStore(STREAM_INTERCEPTOR).get(INTERCEPTORS, ConcurrentMap.class);
    if (interceptors != null) {
      interceptors.values().forEach(DefaultStreamInterceptor::start);
    }
  }

  @Override
  public void afterTestExecution(ExtensionContext context) throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentMap<StreamType, DefaultStreamInterceptor> interceptors =
        context.getStore(STREAM_INTERCEPTOR).get(INTERCEPTORS, ConcurrentMap.class);
    if (interceptors != null) {
      interceptors.values().forEach(DefaultStreamInterceptor::stop);
    }
  }
}
