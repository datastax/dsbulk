/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class StatsSettingsTest {

  @Test
  void should_report_statistics_mode() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("mode = hosts")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.stats")));
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getStatisticsMode()).isEqualTo(StatsSettings.StatisticsMode.hosts);
  }

  @Test
  void should_report_num_partitions() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("numPartitions = 20")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.stats")));
    StatsSettings settings = new StatsSettings(config);
    settings.init();
    assertThat(settings.getNumPartitions()).isEqualTo(20);
  }
}
