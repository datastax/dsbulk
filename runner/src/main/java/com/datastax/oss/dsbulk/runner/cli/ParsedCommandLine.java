/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.runner.cli;

import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;

public class ParsedCommandLine {

  private final WorkflowProvider workflowProvider;
  private final Config config;

  ParsedCommandLine(WorkflowProvider workflowProvider, Config config) {
    this.workflowProvider = workflowProvider;
    this.config = config;
  }

  public WorkflowProvider getWorkflowProvider() {
    return workflowProvider;
  }

  public Config getConfig() {
    return config;
  }
}
