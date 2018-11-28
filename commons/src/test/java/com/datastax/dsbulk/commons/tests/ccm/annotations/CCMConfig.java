/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.ccm.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(TYPE)
public @interface CCMConfig {

  /**
   * The number of nodes to create, per data center. If not set, this defaults to {@code {1}}, i.e.,
   * one data center with one node.
   *
   * @return The number of nodes to create, per data center.
   */
  int[] numberOfNodes() default {1};

  /**
   * Configuration items to add to cassandra.yaml configuration file. Each configuration item must
   * be in the form {@code key:value}.
   *
   * @return Configuration items to add to cassandra.yaml configuration file.
   */
  String[] config() default {
    "write_request_timeout_in_ms:60000",
    "read_request_timeout_in_ms:60000",
    "range_request_timeout_in_ms:60000",
    "request_timeout_in_ms:60000"
  };

  /**
   * Configuration items to add to dse.yaml configuration file. Each configuration item must be in
   * the form {@code key:value}.
   *
   * @return Configuration items to add to dse.yaml configuration file.
   */
  String[] dseConfig() default {};

  /**
   * JVM args to use when starting hosts. System properties should be provided one by one, in the
   * form {@code -Dname=value}.
   *
   * @return JVM args to use when starting hosts.
   */
  String[] jvmArgs() default {};

  /**
   * Free-form options that will be added at the end of the {@code ccm create} command.
   *
   * @return Free-form options that will be added at the end of the {@code ccm create} command.
   */
  String[] createOptions() default {};

  /**
   * Whether to use SSL encryption.
   *
   * @return {@code true} to use encryption, {@code false} to use unencrypted communication
   *     (default).
   */
  boolean ssl() default false;

  /**
   * Whether to use authentication. Implies the use of SSL encryption.
   *
   * @return {@code true} to use authentication, {@code false} to use unauthenticated communication
   *     (default).
   */
  boolean auth() default false;

  /**
   * The workloads to assign to each node. If this attribute is defined, the number of its elements
   * must be lesser than or equal to the number of nodes in the cluster.
   *
   * @return The workloads to assign to each node.
   */
  CCMWorkload[] workloads() default {};
}
