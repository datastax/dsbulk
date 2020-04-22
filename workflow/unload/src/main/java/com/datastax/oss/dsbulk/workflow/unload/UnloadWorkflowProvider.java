/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.unload;

import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

public class UnloadWorkflowProvider implements WorkflowProvider {

  @NonNull
  @Override
  public String getTitle() {
    return "unload";
  }

  @NonNull
  @Override
  public String getDescription() {
    return "Unloads data from DataStax Enterprise or "
        + "Apache Cassandra (R) databases into external data sinks. "
        + "This command requires a connector to write data to; "
        + "the source table, or alternatively, the read query must also be properly configured. "
        + "Run `dsbulk help connector` or `dsbulk help schema` for more information.";
  }

  @NonNull
  @Override
  public Workflow newWorkflow(@NonNull Config config) {
    return new UnloadWorkflow(config);
  }
}
