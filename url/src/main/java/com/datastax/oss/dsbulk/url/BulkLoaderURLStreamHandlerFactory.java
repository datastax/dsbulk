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
package com.datastax.oss.dsbulk.url;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList.Builder;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main factory for URL stream handlers used by DSBulk.
 *
 * <p>This class should be installed as the default URL stream handler factory at application
 * startup.
 *
 * <p>All non-standard URL schemes supported by DSBulk should have a corresponding {@link
 * URLStreamHandlerProvider} registered via the Service Loader API.
 */
public class BulkLoaderURLStreamHandlerFactory implements URLStreamHandlerFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BulkLoaderURLStreamHandlerFactory.class);

  @VisibleForTesting
  static final BulkLoaderURLStreamHandlerFactory INSTANCE = new BulkLoaderURLStreamHandlerFactory();

  private static final AtomicBoolean INSTALLED = new AtomicBoolean(false);

  /**
   * Install this URL stream handler factory as the default factory for the JVM.
   *
   * <p>Installation must be done only once, typically when the JVM starts. Attempting to call this
   * method more than once results in a no-op.
   */
  public static void install() {
    if (INSTALLED.compareAndSet(false, true)) {
      URL.setURLStreamHandlerFactory(INSTANCE);
    }
  }

  private final ImmutableList<URLStreamHandlerProvider> providers;

  private BulkLoaderURLStreamHandlerFactory() {
    // IMPORTANT: the discovery must be done *before* this factory is installed,
    // otherwise the discovery may result in infinite recursion.
    ServiceLoader<URLStreamHandlerProvider> loader =
        ServiceLoader.load(URLStreamHandlerProvider.class);
    Builder<URLStreamHandlerProvider> builder = ImmutableList.builder();
    for (URLStreamHandlerProvider provider : loader) {
      LOGGER.debug("Found URL stream handler provider: {}", provider);
      builder.add(provider);
    }
    providers = builder.build();
  }

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    LOGGER.debug("Creating URL stream handler for protocol: {}", protocol);
    URLStreamHandler handler = null;
    if (protocol != null) {
      for (URLStreamHandlerProvider provider : providers) {
        Optional<URLStreamHandler> maybeHandler = provider.maybeCreateURLStreamHandler(protocol);
        if (maybeHandler.isPresent()) {
          handler = maybeHandler.get();
          break;
        }
      }
    }
    LOGGER.debug("Returning URL stream handler for protocol: {}", handler);
    return handler;
  }
}
