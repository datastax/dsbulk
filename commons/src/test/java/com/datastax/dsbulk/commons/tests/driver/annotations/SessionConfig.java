/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.driver.annotations;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(PARAMETER)
public @interface SessionConfig {

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
