/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.ccm.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.loader.tests.utils.VersionUtils;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** */
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

  /** The C* or DSE version to use; defaults to {@link VersionUtils#DEFAULT_DSE_VERSION}. */
  String version() default VersionUtils.DEFAULT_DSE_VERSION;

  /** Whether to create a DSE cluster or not; defaults to {@code true}. */
  boolean dse() default true;

  /**
   * Configuration items to add to cassandra.yaml configuration file. Each configuration item must
   * be in the form {@code key:value}.
   *
   * @return Configuration items to add to cassandra.yaml configuration file.
   */
  String[] config() default {};

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
