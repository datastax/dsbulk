/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for a Class or Method that defines a DSE Version requirement. If the cassandra version
 * in use does not meet the version requirement, the test is skipped.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface DSERequirement {
  /** @return The minimum version required to execute this test, i.e. "4.8.14" */
  String min() default "";

  /**
   * @return the maximum exclusive version allowed to execute this test, i.e. "5.1.2" means only
   *     tests < "5.1.2" may execute this test.
   */
  String max() default "";

  /** @return The description returned if this version requirement is not met. */
  String description() default "Does not meet version requirement.";
}
