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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URLStreamHandler;
import java.util.Optional;

/**
 * A provider for {@link URLStreamHandler} instances. Provider instances are discovered using the
 * Service Loader API, see {@link BulkLoaderURLStreamHandlerFactory}.
 *
 * <p>This interface and the URL stream handler discovery mechanism used in DSBulk are very close to
 * the mechanism implemented by default in the JDK starting with JDK 9, see {@code
 * java.net.spi.URLStreamHandlerProvider}. However DSBulk still supports Java 8 and thus has to
 * implement its own discovery mechanism.
 */
public interface URLStreamHandlerProvider {

  /**
   * Creates a {@link URLStreamHandler} for the given protocol, if possible, otherwise returns
   * empty.
   *
   * @param protocol The protocol to create a handler for.
   * @return The created handler, or empty if the protocol is not supported.
   */
  @NonNull
  Optional<URLStreamHandler> maybeCreateURLStreamHandler(@NonNull String protocol);
}
