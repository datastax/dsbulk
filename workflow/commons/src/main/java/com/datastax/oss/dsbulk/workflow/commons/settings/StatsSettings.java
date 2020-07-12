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

import com.datastax.oss.dsbulk.config.ConfigUtils;
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

  public StatsSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {
      statisticsModes = config.getEnumList(StatisticsMode.class, MODES);
      numPartitions = config.getInt(NUM_PARTITIONS);
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.stats");
    }
  }

  public EnumSet<StatisticsMode> getStatisticsModes() {
    return EnumSet.copyOf(statisticsModes);
  }

  public int getNumPartitions() {
    return numPartitions;
  }
}
