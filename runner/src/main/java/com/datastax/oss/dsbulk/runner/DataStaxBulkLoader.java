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

import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_ABORTED_FATAL_ERROR;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_CRASHED;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_INTERRUPTED;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_OK;

import com.datastax.oss.dsbulk.commons.url.BulkLoaderURLStreamHandlerFactory;
import com.datastax.oss.dsbulk.commons.utils.ConsoleUtils;
import com.datastax.oss.dsbulk.commons.utils.ThrowableUtils;
import com.datastax.oss.dsbulk.runner.cli.AnsiConfigurator;
import com.datastax.oss.dsbulk.runner.cli.CommandLineParser;
import com.datastax.oss.dsbulk.runner.cli.GlobalHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.ParsedCommandLine;
import com.datastax.oss.dsbulk.runner.cli.SectionHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.VersionRequestException;
import com.datastax.oss.dsbulk.runner.help.HelpEmitter;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import com.typesafe.config.Config;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStaxBulkLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStaxBulkLoader.class);

  public enum ExitStatus {
    STATUS_OK(0),
    STATUS_COMPLETED_WITH_ERRORS(1),
    STATUS_ABORTED_TOO_MANY_ERRORS(2),
    STATUS_ABORTED_FATAL_ERROR(3),
    STATUS_INTERRUPTED(4),
    STATUS_CRASHED(5),
    ;

    private final int exitCode;

    ExitStatus(int exitCode) {
      this.exitCode = exitCode;
    }

    public int exitCode() {
      return exitCode;
    }
  }

  /** A filter to exclude some errors from the sanitized message printed to the console. */
  private static final Predicate<Throwable> NO_REACTOR_ERRORS =
      t ->
          // filter out reactor exceptions as they are usually not relevant to users and
          // other more meaningful errors are generally present.
          !t.getClass().getName().startsWith("reactor.")
              && (t.getMessage() == null || !t.getMessage().startsWith("#block"));

  private final String[] args;

  public static void main(String[] args) {
    URL.setURLStreamHandlerFactory(new BulkLoaderURLStreamHandlerFactory());
    ExitStatus status = new DataStaxBulkLoader(args).run();
    System.exit(status.exitCode());
  }

  public DataStaxBulkLoader(String... args) {
    this.args = args;
  }

  public ExitStatus run() {

    Workflow workflow = null;
    try {

      AnsiConfigurator.configureAnsi(args);

      CommandLineParser parser = new CommandLineParser(args);
      ParsedCommandLine result = parser.parse();
      Config config = result.getConfig();
      workflow = result.getWorkflowProvider().newWorkflow(config);

      WorkflowThread workflowThread = new WorkflowThread(workflow);
      Runtime.getRuntime().addShutdownHook(new CleanupThread(workflow, workflowThread));

      // start the workflow and wait for its completion
      workflowThread.start();
      workflowThread.join();
      return workflowThread.status;

    } catch (GlobalHelpRequestException e) {
      HelpEmitter.emitGlobalHelp(e.getConnectorName());
      return STATUS_OK;

    } catch (SectionHelpRequestException e) {
      try {
        HelpEmitter.emitSectionHelp(e.getSectionName(), e.getConnectorName());
        return STATUS_OK;
      } catch (Exception e2) {
        LOGGER.error(e2.getMessage(), e2);
        return STATUS_CRASHED;
      }

    } catch (VersionRequestException e) {
      // Use the OS charset
      PrintWriter pw =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(System.out, Charset.defaultCharset())));
      pw.println(ConsoleUtils.getBulkLoaderNameAndVersion());
      pw.flush();
      return STATUS_OK;

    } catch (Throwable t) {
      return handleUnexpectedError(workflow, t);
    }
  }

  /**
   * A thread responsible for running the workflow.
   *
   * <p>We run the workflow on a dedicated thread to be able to interrupt it when the JVM receives a
   * SIGINT signal (CTRL + C).
   *
   * @see CleanupThread
   */
  private static class WorkflowThread extends Thread {

    private final Workflow workflow;
    private volatile ExitStatus status = null;

    private WorkflowThread(Workflow workflow) {
      super("workflow-runner");
      this.workflow = workflow;
    }

    @Override
    public void run() {
      try {
        workflow.init();
        status = workflow.execute() ? STATUS_OK : STATUS_COMPLETED_WITH_ERRORS;
      } catch (TooManyErrorsException e) {
        LOGGER.error(workflow + " aborted: " + e.getMessage(), e);
        status = STATUS_ABORTED_TOO_MANY_ERRORS;
      } catch (Throwable error) {
        status = handleUnexpectedError(workflow, error);
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
  }

  /**
   * A shutdown hook responsible for interrupting the workflow thread in the event of a user
   * interruption via SIGINT (CTLR + C), and for closing the workflow.
   */
  private static class CleanupThread extends Thread {

    private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(10);

    private final Workflow workflow;
    private final WorkflowThread workflowThread;

    private CleanupThread(Workflow workflow, WorkflowThread workflowThread) {
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
            workflowThread.status = STATUS_CRASHED;
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

  private static ExitStatus handleUnexpectedError(Workflow workflow, Throwable error) {
    // Reactor framework often wraps InterruptedException.
    if (ThrowableUtils.isInterrupted(error)) {
      return STATUS_INTERRUPTED;
    } else {
      String errorMessage = ThrowableUtils.getSanitizedErrorMessage(error, NO_REACTOR_ERRORS, 2);
      String operationName = workflow == null ? "Operation" : workflow.toString();
      if (error instanceof Exception) {
        LOGGER.error(operationName + " failed: " + errorMessage, error);
        return STATUS_ABORTED_FATAL_ERROR;
      } else {
        LOGGER.error(operationName + " failed unexpectedly: " + errorMessage, error);
        return STATUS_CRASHED;
      }
    }
  }
}
