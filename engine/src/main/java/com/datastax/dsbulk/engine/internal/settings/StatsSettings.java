/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.typesafe.config.ConfigException;

public class StatsSettings {

  public enum StatisticsMode {
    global,
    ranges,
    hosts,
    partitions,
    all
  }

  private static final String MODE = "mode";
  private static final String NUM_PARTITIONS = "numPartitions";

  private final LoaderConfig config;

  private StatisticsMode statisticsMode;
  private int numPartitions;

  StatsSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      statisticsMode = config.getEnum(StatisticsMode.class, MODE);
      numPartitions = config.getInt(NUM_PARTITIONS);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "stats");
    } catch (IllegalArgumentException e) {
      throw new BulkConfigurationException(e);
    }
  }

  public StatisticsMode getStatisticsMode() {
    return statisticsMode;
  }

  public int getNumPartitions() {
    return numPartitions;
  }
}
