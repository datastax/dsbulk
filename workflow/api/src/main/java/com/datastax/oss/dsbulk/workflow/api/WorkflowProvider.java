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
