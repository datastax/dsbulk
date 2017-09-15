/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.config.DSBulkConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultDSBulkConfig;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/** */
public class EngineSettingsTest {

  @Test
  public void should_create_default_engine_settings() throws Exception {
    DSBulkConfig config = new DefaultDSBulkConfig(ConfigFactory.load().getConfig("dsbulk.engine"));
    EngineSettings settings = new EngineSettings(config);
    assertThat(settings.getMaxMappingThreads())
        .isEqualTo(Runtime.getRuntime().availableProcessors());
    assertThat(settings.getMaxConcurrentReads()).isEqualTo(4);
  }

  @Test
  public void should_create_custom_engine_settings() throws Exception {
    DSBulkConfig config =
        new DefaultDSBulkConfig(
            ConfigFactory.parseString("maxMappingThreads = 8C, maxConcurrentReads = 4C")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.engine")));
    EngineSettings settings = new EngineSettings(config);
    assertThat(settings.getMaxMappingThreads())
        .isEqualTo(Runtime.getRuntime().availableProcessors() * 8);
    assertThat(settings.getMaxConcurrentReads())
        .isEqualTo(Runtime.getRuntime().availableProcessors() * 4);
  }
}
