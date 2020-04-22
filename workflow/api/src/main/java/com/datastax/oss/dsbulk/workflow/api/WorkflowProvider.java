/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.api;

import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A provider for {@link Workflow}s. Providers are automatically discovered with the Service Loader
 * API by the runner, see dsbulk-runner module.
 */
public interface WorkflowProvider {

  /**
   * @return A descriptive title for the type of workflows this provider creates. Will be displayed
   *     in help texts.
   */
  @NonNull
  String getTitle();

  /**
   * @return A detailed description for the type of workflows this provider creates. Will be
   *     displayed in help texts.
   */
  @NonNull
  String getDescription();

  /**
   * Creates a new {@link Workflow} with the given configuration and shortcuts map. See
   * dsbulk-runner.
   */
  @NonNull
  Workflow newWorkflow(@NonNull Config config);
}
