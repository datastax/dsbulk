/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

/** */
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
