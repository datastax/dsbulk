/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.typesafe.config.ConfigException;

public class EngineSettings {

  public enum StatisticsMode {
    global,
    ranges,
    hosts,
    all
  }

  private static final String DRY_RUN = "dryRun";
  private static final String EXECUTION_ID = "executionId";

  private static final String STATS = "stats";

  private final LoaderConfig config;
  private boolean dryRun;
  private String executionId;

  private StatisticsMode statisticsMode;

  EngineSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      dryRun = config.getBoolean(DRY_RUN);
      executionId = config.getString(EXECUTION_ID);
      statisticsMode = config.getEnum(StatisticsMode.class, STATS);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "engine");
    }
  }

  public boolean isDryRun() {
    return dryRun;
  }

  String getCustomExecutionIdTemplate() {
    return executionId;
  }

  public StatisticsMode getStatisticsMode() {
    return statisticsMode;
  }
}
