/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.simulacron.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method as being a factory for {@link
 * com.datastax.oss.simulacron.server.BoundCluster Simulacron} instances.
 *
 * <p>Such methods must be static and their return type must be {@link
 * com.datastax.oss.simulacron.common.cluster.ClusterSpec}.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface SimulacronFactory {}
