/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.commons.internal.utils.ThrowableUtils.getSanitizedErrorMessage;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.NO_REACTOR_ERRORS;

import com.datastax.dsbulk.commons.internal.utils.ThrowableUtils;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.engine.internal.cli.AnsiConfigurator;
import com.datastax.dsbulk.engine.internal.cli.CommandLineParser;
import com.datastax.dsbulk.engine.internal.cli.GlobalHelpRequestException;
import com.datastax.dsbulk.engine.internal.cli.ParsedCommandLine;
import com.datastax.dsbulk.engine.internal.cli.SectionHelpRequestException;
import com.datastax.dsbulk.engine.internal.cli.VersionRequestException;
import com.datastax.dsbulk.engine.internal.help.HelpEmitter;
import com.datastax.dsbulk.engine.internal.log.TooManyErrorsException;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.typesafe.config.Config;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStaxBulkLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStaxBulkLoader.class);

  public static final int STATUS_OK = 0;
  public static final int STATUS_COMPLETED_WITH_ERRORS = 1;
  public static final int STATUS_ABORTED_TOO_MANY_ERRORS = 2;
  public static final int STATUS_ABORTED_FATAL_ERROR = 3;
  public static final int STATUS_INTERRUPTED = 4;
  public static final int STATUS_CRASHED = 5;

  private final String[] args;

  public static void main(String[] args) {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    int status = new DataStaxBulkLoader(args).run();
    System.exit(status);
  }

  public DataStaxBulkLoader(String... args) {
    this.args = args;
  }

  public int run() {

    Workflow workflow = null;
    try {

      AnsiConfigurator.configureAnsi(args);

      ParsedCommandLine result = new CommandLineParser(args).parse();
      Config config = result.getConfig();
      BiMap<String, String> shortcuts = result.getShortcuts();
      workflow = result.getWorkflowType().newWorkflow(config, shortcuts);

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
      pw.println(WorkflowUtils.getBulkLoaderNameAndVersion());
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
    private volatile int status = -1;

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

  private static int handleUnexpectedError(Workflow workflow, Throwable error) {
    // Reactor framework often wraps InterruptedException.
    if (ThrowableUtils.isInterrupted(error)) {
      return STATUS_INTERRUPTED;
    } else {
      String errorMessage = getSanitizedErrorMessage(error, NO_REACTOR_ERRORS, 2);
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
