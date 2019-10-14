/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.cloud;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;

/**
 * An abstraction around an SNI Proxy server acting as an entry point to a DataStax Apollo Cloud
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
  String getLocalDCName();
}
