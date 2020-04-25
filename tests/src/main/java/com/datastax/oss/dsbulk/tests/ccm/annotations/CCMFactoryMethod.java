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

import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.DefaultCCMCluster.Builder;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A class annotated with this annotation should obtain its {@link CCMCluster} instance from the
 * specified factory method.
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface CCMFactoryMethod {

  /**
   * Returns the name of the method that should be invoked to obtain a {@link Builder} instance.
   *
   * <p>This method should be declared in {@link #factoryClass()}, or if that attribute is not set,
   * it will be looked up on the test class itself.
   *
   * <p>The method should not have parameters. It can be static or not, and have any visibility.
   *
   * <p>By default, a {@link Builder} instance is obtained by parsing other attributes of this
   * annotation.
   *
   * @return The name of the method that should be invoked to obtain a {@link Builder} instance.
   */
  String value() default "";

  /**
   * Returns the name of the class that should be invoked to obtain a {@link Builder} instance.
   *
   * <p>This class should contain a method named after {@link #value()}; if this attribute is not
   * set, it will default to the test class itself.
   *
   * @return The name of the class that should be invoked to obtain a {@link Builder} instance.
   */
  Class<?> factoryClass() default TestClass.class;

  /** A dummy class that acts like a placeholder for the actual test class being executed. */
  final class TestClass {}
}
