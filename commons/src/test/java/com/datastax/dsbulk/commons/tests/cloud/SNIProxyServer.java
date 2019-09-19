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
 * An abstraction around a Cassandra or DSE cluster managed by the <a
 * href="https://github.com/pcmanus/ccm">CCM tool</a>.
 */
public interface SNIProxyServer extends Closeable {

  void start();

  void stop();

  /**
   * Closes the cluster. This is usually a synonym of {@link #stop()} to comply with {@link
   * Closeable} interface.
   */
  @Override
  default void close() {
    stop();
  }

  Path getSecureBundlePath();

  List<EndPoint> getContactPoints();

  String getLocalDCName();
}
