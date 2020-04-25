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
package com.datastax.oss.dsbulk.tests.ccm.annotations;

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
  String[] dseConfig() default {"cql_slow_log_options.enabled:false"};

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
   * Whether clients (tests) will use hostname verification when using SSL encryption.
   *
   * @return {@code true} to indicate client will verify the server cert, false otherwise (default).
   */
  boolean hostnameVerification() default false;

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
