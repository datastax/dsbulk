/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
