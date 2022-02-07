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
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;

import com.datastax.oss.dsbulk.runner.cli.AnsiConfigurator;
import com.datastax.oss.dsbulk.runner.cli.CommandLineParser;
import com.datastax.oss.dsbulk.runner.cli.GlobalHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.ParsedCommandLine;
import com.datastax.oss.dsbulk.runner.cli.SectionHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.VersionRequestException;
import com.datastax.oss.dsbulk.runner.help.HelpEmitter;
import com.datastax.oss.dsbulk.url.BulkLoaderURLStreamHandlerFactory;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.utils.WorkflowUtils;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The main entry point for command line invocations of DSBulk. */
public class DataStaxBulkLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStaxBulkLoader.class);

  private final String[] args;

  public static void main(String[] args) {
    BulkLoaderURLStreamHandlerFactory.install();
    ExitStatus status = new DataStaxBulkLoader(args).run();
    System.exit(status.exitCode());
  }

  public DataStaxBulkLoader(String... args) {
    this.args = args;
  }

  @NonNull
  public ExitStatus run() {

    Workflow workflow = null;
    try {

      AnsiConfigurator.configureAnsi(args);

      CommandLineParser parser = new CommandLineParser(args);
      ParsedCommandLine result = parser.parse();
      Config config = result.getConfig();
      BulkLoaderURLStreamHandlerFactory.setConfig(config);
      workflow = result.getWorkflowProvider().newWorkflow(config);

      WorkflowThread workflowThread = new WorkflowThread(workflow);
      Runtime.getRuntime().addShutdownHook(new CleanupThread(workflow, workflowThread));

      // start the workflow and wait for its completion
      workflowThread.start();
      workflowThread.join();

      return workflowThread.getExitStatus();

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
      return ErrorHandler.handleUnexpectedError(workflow, t);
    }
  }
}
