/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.simulacron.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** */
@Retention(RUNTIME)
@Target(TYPE)
public @interface SimulacronConfig {

  /**
   * The number of nodes to create, per data center. If not set, this defaults to {@code {1}}, i.e.,
   * one data center with one node.
   *
   * @return The number of nodes to create, per data center.
   */
  int[] numberOfNodes() default {1};

  String version() default "";

  boolean dse() default true;

  int numberOfTokens() default 1;

  /**
   * Peer info items to add to the peers table. Each configuration item must be in the form {@code
   * key:value}.
   *
   * @return Peer info items to add to the peers table.
   */
  String[] peerInfo() default {};
}
