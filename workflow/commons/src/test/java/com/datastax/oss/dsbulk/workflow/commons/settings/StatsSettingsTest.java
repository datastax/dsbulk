/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
