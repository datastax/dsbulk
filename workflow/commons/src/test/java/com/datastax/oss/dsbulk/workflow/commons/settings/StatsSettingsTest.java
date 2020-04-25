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

import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode.ranges;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class StatsSettingsTest {

  @Test
  void should_report_statistics_mode() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.stats", "modes", "[hosts,ranges,partitions,global]");
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getStatisticsModes()).contains(hosts, ranges, partitions, global);
  }

  @Test
  void should_report_num_partitions() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.stats", "numPartitions", 20);
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getNumPartitions()).isEqualTo(20);
  }
}
