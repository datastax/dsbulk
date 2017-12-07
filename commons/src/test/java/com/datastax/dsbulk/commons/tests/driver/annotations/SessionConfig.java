/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.driver.annotations;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(PARAMETER)
public @interface SessionConfig {

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
   * The strategy to adopt when initializing a {@link com.datastax.driver.core.Session}: whether to
   * create a new random keyspace, a keyspace with a fixed name, or no keyspace.
   */
  enum UseKeyspaceMode {
    NONE,
    GENERATE,
    FIXED
  }
}
