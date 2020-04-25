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
package com.datastax.oss.dsbulk.workflow.count;

import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

public class CountWorkflowProvider implements WorkflowProvider {

  @NonNull
  @Override
  public String getTitle() {
    return "count";
  }

  @NonNull
  @Override
  public String getDescription() {
    return "Computes statistics about a table, such as "
        + "the total number of rows, "
        + "the number of rows per token range, "
        + "the number of rows per host, "
        + "or the total number of rows in the N biggest partitions in the table. "
        + "Run `dsbulk help stats` for more information.";
  }

  @NonNull
  @Override
  public Workflow newWorkflow(@NonNull Config config) {
    return new CountWorkflow(config);
  }
}
