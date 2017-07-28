/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.ccm.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.driver.core.ProtocolOptions;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface ClusterConfig {

  /**
   * The credentials to use. If set, the returned array should contain 2 elements, the first one
   * being the username, the second one being the password.
   *
   * @return The credentials to use.
   */
  String[] credentials() default {};

  /** Whether to enable metrics. */
  boolean metrics() default true;

  /** Whether to enable SSL. */
  boolean ssl() default false;

  /** Whether to enable JMX. */
  boolean jmx() default false;

  /** The protocol version to use. */
  ProtocolVersion protocolVersion() default ProtocolVersion.NEGOTIATE;

  /** The compression to use. */
  ProtocolOptions.Compression compression() default ProtocolOptions.Compression.NONE;

  /**
   * PoolingOptions options to set.
   *
   * <p>Each option item must be in the form {@code key:value}. where {@code key} is a property name
   * in {@link com.datastax.driver.core.PoolingOptions} and {@code value} is the value to set.
   *
   * <p>If the property setter method takes more than one argument, then {@code value} can contain
   * several values separated by commas, e.g. {@code setConnectionsPerHost:LOCAL,2,8}.
   */
  String[] poolingOptions() default {};

  /**
   * SocketOptions options to set.
   *
   * <p>Each option item must be in the form {@code key:value}. where {@code key} is a property name
   * in {@link com.datastax.driver.core.SocketOptions} and {@code value} is the value to set.
   *
   * <p>If the property setter method takes more than one argument, then {@code value} can contain
   * several values separated by commas, e.g. {@code coreConnectionsPerHost:LOCAL,8}.
   */
  String[] socketOptions() default {};

  /**
   * QueryOptions options to set.
   *
   * <p>Each option item must be in the form {@code key:value}. where {@code key} is a property name
   * in {@link com.datastax.driver.core.QueryOptions} and {@code value} is the value to set.
   *
   * <p>If the property setter method takes more than one argument, then {@code value} can contain
   * several values separated by commas, e.g. {@code coreConnectionsPerHost:LOCAL,8}.
   */
  String[] queryOptions() default {};

  /**
   * Graph options to set (DSE-specific feature).
   *
   * <p>Each option item must be in the form {@code key:value}. where {@code key} is a property name
   * in {@link com.datastax.driver.dse.graph.GraphOptions} and {@code value} is the value to set.
   *
   * <p>If the property setter method takes more than one argument, then {@code value} can contain
   * several values separated by commas, e.g. {@code coreConnectionsPerHost:LOCAL,8}.
   */
  String[] graphOptions() default {};

  /**
   * The protocol version to use when creating {@link com.datastax.driver.core.Cluster} instances.
   */
  enum ProtocolVersion {
    V1 {
      @Override
      public com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion() {
        return com.datastax.driver.core.ProtocolVersion.V1;
      }
    },
    V2 {
      @Override
      public com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion() {
        return com.datastax.driver.core.ProtocolVersion.V2;
      }
    },
    V3 {
      @Override
      public com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion() {
        return com.datastax.driver.core.ProtocolVersion.V3;
      }
    },
    V4 {
      @Override
      public com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion() {
        return com.datastax.driver.core.ProtocolVersion.V4;
      }
    },
    NEGOTIATE {
      @Override
      public com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion() {
        return null;
      }
    };

    public abstract com.datastax.driver.core.ProtocolVersion toDriverProtocolVersion();
  }
}
