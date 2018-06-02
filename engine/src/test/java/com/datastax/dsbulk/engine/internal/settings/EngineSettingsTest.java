/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class EngineSettingsTest {

  @Test
  void should_report_default_dry_run() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.engine"));
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isFalse();
  }

  @Test
  void should_create_custom_dry_run() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("dryRun = true")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.engine")));
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isTrue();
  }
}
