/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests.utils;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

/** */
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
