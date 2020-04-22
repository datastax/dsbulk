/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.api;

import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;

/** Defines a pipeline of transformations to apply. */
public interface Workflow extends AutoCloseable {

  /**
   * Initializes the workflow.
   *
   * <p>This method is guaranteed to be called once and only once by the execution engine.
   *
   * @throws Exception if the workflow could not be initialized properly.
   */
  void init() throws Exception;

  /**
   * Executes the workflow.
   *
   * <p>This method is guaranteed to be called once and only once by the execution engine, and only
   * after the workflow has been properly {@linkplain #init() initialized}.
   *
   * @return {@code true} if the workflow completed without any errors, {@code false} if it
   *     completed with errors.
   * @throws TooManyErrorsException if the workflow encountered too many non-fatal errors and
   *     decided to stop.
   * @throws Exception if the workflow encountered a fatal error that prevented it from completing.
   */
  boolean execute() throws TooManyErrorsException, Exception;

  /**
   * Closes the workflow and releases all open resources (file descriptors, socket connections,
   * etc.).
   *
   * <p>This method is guaranteed to be called once and only once by the execution engine, and only
   * after the workflow has finished its {@link #execute() execution}, regardless of whether the
   * execution finished normally or exceptionally.
   *
   * @throws Exception if the workflow encountered an error while closing open resources.
   */
  @Override
  void close() throws Exception;
}
