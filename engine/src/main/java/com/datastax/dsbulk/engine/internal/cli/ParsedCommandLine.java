/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.cli;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;

public class ParsedCommandLine {

  private final WorkflowType workflowType;
  private final LoaderConfig config;
  private final BiMap<String, String> shortcuts;

  ParsedCommandLine(
      WorkflowType workflowType, LoaderConfig config, BiMap<String, String> shortcuts) {
    this.workflowType = workflowType;
    this.config = config;
    this.shortcuts = shortcuts;
  }

  public WorkflowType getWorkflowType() {
    return workflowType;
  }

  public LoaderConfig getConfig() {
    return config;
  }

  public BiMap<String, String> getShortcuts() {
    return shortcuts;
  }
}
