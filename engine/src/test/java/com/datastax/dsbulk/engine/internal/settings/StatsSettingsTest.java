/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import org.junit.jupiter.api.Test;

class StatsSettingsTest {

  @Test
  void should_report_statistics_mode() {
    LoaderConfig config =
        createTestConfig("dsbulk.stats", "modes", "[hosts,ranges,partitions,global]");
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getStatisticsModes()).contains(hosts, ranges, partitions, global);
  }

  @Test
  void should_report_num_partitions() {
    LoaderConfig config = createTestConfig("dsbulk.stats", "numPartitions", 20);
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getNumPartitions()).isEqualTo(20);
  }
}
