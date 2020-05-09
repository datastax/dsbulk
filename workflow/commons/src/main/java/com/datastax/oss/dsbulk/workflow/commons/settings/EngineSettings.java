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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class EngineSettings {

  private static final String DRY_RUN = "dryRun";
  private static final String EXECUTION_ID = "executionId";

  private final Config config;
  private boolean dryRun;
  private String executionId;

  EngineSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {
      dryRun = config.getBoolean(DRY_RUN);
      executionId = config.hasPath(EXECUTION_ID) ? config.getString(EXECUTION_ID) : null;
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.engine");
    }
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public String getCustomExecutionIdTemplate() {
    return executionId;
  }
}
