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
package com.datastax.oss.dsbulk.workflow.api.config;

import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A post-processor for {@link Config} objects.
 *
 * <p>Config post-processors are used by DSBulk to allow external components to customize the
 * workflow configuration. Post-processors are discovered using the Service Loader API and are
 * invoked immediately after the command line parsing, but before the workflow is started.
 *
 * <p>Post-processors are useful to validate the final config, or to enrich the config with derived
 * settings.
 */
public interface ConfigPostProcessor {

  /**
   * Post-process the given {@link Config} and return the processed, resulting config – which may be
   * the same object, or a different one.
   *
   * @param config The {@link Config} to post-process; it will be {@linkplain Config#resolve()
   *     resolved} already.
   * @return The post-processed config.
   */
  @NonNull
  Config postProcess(@NonNull Config config);
}
