/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.ccm.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.loader.tests.ccm.CCMCluster;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** Marks the test class as requiring a {@link CCMCluster}. */
@Retention(RUNTIME)
@Target(TYPE)
public @interface CCMTest {

  /** A brief description of the test and why it requires CCM, for documentation purposes. */
  String value() default "";
}
