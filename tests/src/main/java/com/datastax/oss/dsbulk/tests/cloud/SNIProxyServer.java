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
package com.datastax.oss.dsbulk.tests.cloud;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;

/**
 * An abstraction around an SNI Proxy server acting as an entry point to a DataStax Astra Cloud
 * database.
 *
 * <p>The SNI proxy server Docker image must be previously installed prior to using this
 * abstraction.
 *
 * @see <a href="https://github.com/riptano/sni_single_endpoint">SNI single endpoint Github repo</a>
 */
public interface SNIProxyServer extends Closeable {

  /** Starts the cluster and the proxy server. */
  void start();

  /** Stops the cluster and the proxy server. */
  void stop();

  /**
   * Stops the cluster and the proxy server. This is usually a synonym of {@link #stop()} to comply
   * with {@link Closeable} interface.
   */
  @Override
  default void close() {
    stop();
  }

  /** @return The path to the cloud secure connect bundle to use to connect to this cluster. */
  Path getSecureBundlePath();

  /** @return The endpoints to use to connect to this cluster. */
  List<EndPoint> getContactPoints();

  /** @return The cluster's local DC name. */
  String getLocalDatacenter();
}
