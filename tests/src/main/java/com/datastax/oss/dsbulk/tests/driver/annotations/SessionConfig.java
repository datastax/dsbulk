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
package com.datastax.oss.dsbulk.tests.driver.annotations;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(PARAMETER)
public @interface SessionConfig {

  /** The session configuration; each entry is of the form setting=value. */
  String[] settings() default {};

  /**
   * The credentials to use. If set, the returned array should contain 2 elements, the first one
   * being the username, the second one being the password.
   *
   * @return The credentials to use.
   */
  String[] credentials() default {};

  /** Whether to enable SSL with a default set of credentials, keystores and truststores. */
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
   * Whether to set the current session to a specific keyspace through a {@code USE} statement.
   *
   * <p>This will be done only once, upon session creation, and should not be changed afterwards in
   * a test.
   *
   * <p>The possible values are:
   *
   * <ol>
   *   <li>{@link UseKeyspaceMode#NONE} will not create any keyspace, and the session will not be
   *       bound to any keyspace
   *   <li>{@link UseKeyspaceMode#GENERATE} will generate a random keyspace name;
   *   <li>{@link UseKeyspaceMode#FIXED} will generate a fixed keyspace name after {@link
   *       #loggedKeyspaceName()};
   * </ol>
   *
   * <p>Otherwise, the name specified here will be the keyspace name to create and {@code USE}.
   *
   * @return Whether to create and {@code USE} a keyspace for this session.
   */
  UseKeyspaceMode useKeyspace() default UseKeyspaceMode.GENERATE;

  /**
   * @return The keyspace name to create. Useful only when {@link #useKeyspace()} is {@link
   *     UseKeyspaceMode#FIXED}.
   */
  String loggedKeyspaceName() default "";

  /**
   * The strategy to adopt when initializing a {@link com.datastax.oss.driver.api.core.CqlSession}:
   * whether to create a new random keyspace, a keyspace with a fixed name, or no keyspace.
   */
  enum UseKeyspaceMode {
    NONE,
    GENERATE,
    FIXED
  }
}
