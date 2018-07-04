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
public @interface SessionFactoryMethod {

  /**
   * Returns the name of the method that should be invoked to obtain a {@link
   * com.datastax.oss.driver.api.core.CqlSessionBuilder} instance.
   *
   * <p>This method should be declared in {@link #factoryClass()}, or if that attribute is not set,
   * it will be looked up on the test class itself.
   *
   * <p>The method should not have parameters. It can be static or not, and have any visibility.
   *
   * @return The name of the method that should be invoked to obtain a {@link
   *     com.datastax.oss.driver.api.core.CqlSessionBuilder} instance.
   */
  String value();

  /**
   * Returns the name of the class that should be invoked to obtain a {@link
   * com.datastax.oss.driver.api.core.CqlSessionBuilder} instance.
   *
   * <p>This class should contain a method named after {@link #value()}; if this attribute is not
   * set, it will default to the test class itself.
   *
   * @return The name of the class that should be invoked to obtain a {@link
   *     com.datastax.oss.driver.api.core.CqlSessionBuilder} instance.
   */
  Class<?> factoryClass() default TestClass.class;

  /** A dummy class that acts like a placeholder for the actual test class being executed. */
  final class TestClass {}
}
