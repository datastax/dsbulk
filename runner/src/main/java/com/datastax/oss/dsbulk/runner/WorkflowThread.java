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
package com.datastax.oss.dsbulk.runner;

import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;

import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread responsible for running the workflow.
 *
 * <p>We run the workflow on a dedicated thread to be able to interrupt it when the JVM receives a
 * SIGINT signal (CTRL + C).
 *
 * @see CleanupThread
 */
public class WorkflowThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowThread.class);

  private final Workflow workflow;

  private volatile ExitStatus exitStatus;

  public WorkflowThread(@NonNull Workflow workflow) {
    super("workflow-runner");
    this.workflow = workflow;
  }

  @Override
  public void run() {
    try {
      workflow.init();
      exitStatus = workflow.execute() ? STATUS_OK : STATUS_COMPLETED_WITH_ERRORS;
    } catch (TooManyErrorsException e) {
      LOGGER.error(workflow + " aborted: " + e.getMessage(), e);
      exitStatus = STATUS_ABORTED_TOO_MANY_ERRORS;
    } catch (Throwable error) {
      exitStatus = ErrorHandler.handleUnexpectedError(workflow, error);
    } finally {
      try {
        // make sure System.err is flushed before the closing sequence is printed to System.out
        System.out.flush();
        System.err.flush();
        workflow.close();
      } catch (Exception e) {
        LOGGER.error(String.format("%s could not be closed.", workflow), e);
      }
    }
  }

  public ExitStatus getExitStatus() {
    return exitStatus;
  }

  public void setExitStatus(ExitStatus exitStatus) {
    this.exitStatus = exitStatus;
  }
}
