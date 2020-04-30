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
package com.datastax.oss.dsbulk.tests.simulacron.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

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

  /**
   * @return The Cassandra version the cluster should report. Setting this to a 4-part version will
   *     imply a DSE cluster.
   */
  String cassandraVersion() default "4.0.0.2284";

  /**
   * @return The DSE version the cluster should report. Setting this to empty will imply a non-DSE
   *     cluster.
   */
  String dseVersion() default "6.0.0";

  /** @return The number of virtual nodes (tokens) assigned for each node of each datacenter. */
  int numberOfTokens() default 1;

  /**
   * Peer info items to add to the peers table. Each configuration item must be in the form {@code
   * key:value}.
   *
   * @return Peer info items to add to the peers table.
   */
  String[] peerInfo() default {};
}
