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

import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_CRASHED;

import com.datastax.oss.dsbulk.workflow.api.Workflow;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shutdown hook responsible for interrupting the workflow thread in the event of a user
 * interruption via SIGINT (CTLR + C), and for closing the workflow.
 */
public class CleanupThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(CleanupThread.class);

  private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(10);

  private final Workflow workflow;

  private final WorkflowThread workflowThread;

  public CleanupThread(@NonNull Workflow workflow, @NonNull WorkflowThread workflowThread) {
    super("cleanup-thread");
    this.workflow = workflow;
    this.workflowThread = workflowThread;
  }

  @Override
  public void run() {
    try {
      if (workflowThread.isAlive()) {
        LOGGER.error(workflow + " interrupted, waiting for termination.");
        workflowThread.interrupt();
        workflowThread.join(SHUTDOWN_GRACE_PERIOD.toMillis());
        if (workflowThread.isAlive()) {
          workflowThread.setExitStatus(STATUS_CRASHED);
          LOGGER.error(
              String.format(
                  "%s did not terminate within %d seconds, forcing termination.",
                  workflow, SHUTDOWN_GRACE_PERIOD.getSeconds()));
        }
      }
      // make sure System.err is flushed before the closing sequence is printed to System.out
      System.out.flush();
      System.err.flush();
      workflow.close();
    } catch (Exception e) {
      LOGGER.error(String.format("%s could not be closed.", workflow), e);
    }
  }
}
