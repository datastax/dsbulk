/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.ccm.annotations;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Workload;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** A set of workloads to assign to a specific node. */
@Retention(RUNTIME)
@Target(ANNOTATION_TYPE)
public @interface CCMWorkload {

  /**
   * The workloads to assign to a specific node.
   *
   * @return The workloads to assign to a specifc node.
   */
  Workload[] value() default {};
}
