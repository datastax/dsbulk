/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method as being a factory for {@link com.datastax.driver.core.Cluster}
 * instances.
 *
 * <p>Such methods must be static and their return type must be {@link
 * com.datastax.driver.core.Cluster.Builder}.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface ClusterFactory {}
