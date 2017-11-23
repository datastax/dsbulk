/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests;

import com.datastax.driver.core.*;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import org.junit.rules.ExternalResource;

/**
 * An {@link ExternalResource} which provides a {@link #setUp()} method for initializing the
 * resource externally (instead of making users use rule chains) and a {@link #getContactPoints()}
 * for accessing the contact points of the cassandra cluster.
 */
public abstract class CassandraResourceRule extends ExternalResource {

  public synchronized void setUp() {
    try {
      this.before();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * @return Default contact points associated with this cassandra resource. By default returns
   *     127.0.0.1
   */
  public Set<InetSocketAddress> getContactPoints() {
    return Collections.singleton(new InetSocketAddress("127.0.0.1", 9042));
  }

  /** @return The highest protocol version supported by this resource. */
  public abstract ProtocolVersion getHighestProtocolVersion();
}
