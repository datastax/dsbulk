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
