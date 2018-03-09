/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

@Retention(RUNTIME)
public @interface VersionRequirement {

  /** @return The minimum version required to execute this test (inclusive), i.e. "4.8.14" */
  String min() default "";

  /**
   * @return the maximum exclusive version allowed to execute this test, i.e. "5.1.2" means only
   *     tests < "5.1.2" may execute this test.
   */
  String max() default "";
}
