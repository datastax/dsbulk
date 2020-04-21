/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
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
      throw ConfigUtils.fromTypeSafeConfigException(e, "dsbulk.engine");
    }
  }

  public boolean isDryRun() {
    return dryRun;
  }

  String getCustomExecutionIdTemplate() {
    return executionId;
  }
}
