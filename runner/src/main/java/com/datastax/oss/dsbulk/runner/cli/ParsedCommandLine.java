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
