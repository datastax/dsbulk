/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.EnumSet;
import java.util.List;

public class StatsSettings {

  public enum StatisticsMode {
    global,
    ranges,
    hosts,
    partitions
  }

  private static final String MODES = "modes";
  private static final String NUM_PARTITIONS = "numPartitions";

  private final Config config;

  private List<StatisticsMode> statisticsModes;
  private int numPartitions;

  StatsSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {
      statisticsModes = config.getEnumList(StatisticsMode.class, MODES);
      numPartitions = config.getInt(NUM_PARTITIONS);
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.stats");
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public EnumSet<StatisticsMode> getStatisticsModes() {
    return EnumSet.copyOf(statisticsModes);
  }

  public int getNumPartitions() {
    return numPartitions;
  }
}
