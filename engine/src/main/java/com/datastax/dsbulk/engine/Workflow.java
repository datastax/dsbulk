/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

/**
 * Defines a pipeline of transformations to apply.
 *
 * <p>Currently two workflows are defined: {@link LoadWorkflow load} and {@link UnloadWorkflow
 * unload}.
 *
 * @see LoadWorkflow
 * @see UnloadWorkflow
 */
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
   * after the workflow has been properly {@link #init() initialized}.
   *
   * @return {@code true} if the workflow completed without any errors, {@code false} if it
   *     completed with errors.
   * @throws Exception if the workflow encountered a fatal error that prevented it from completing.
   */
  boolean execute() throws Exception;

  /**
   * CLoses the workflow and releases all open resources (file descriptors, socket connections,
   * etc.).
   *
   * <p>This method is guaranteed to be called once and only once by the execution engine, and only
   * after the workflow has finished its {@link #execute() execution}, regardless of whether the
   * execution finished normally or exceptionally.
   *
   * @throws Exception if the engine workflow an error while closing open resources.
   */
  @Override
  void close() throws Exception;
}
