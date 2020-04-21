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
import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class EngineSettingsTest {

  @Test
  void should_report_default_dry_run() {
    Config config = createTestConfig("dsbulk.engine");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isFalse();
  }

  @Test
  void should_create_custom_dry_run() {
    Config config = createTestConfig("dsbulk.engine", "dryRun", true);
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isTrue();
  }

  @Test
  void should_report_execution_id() {
    Config config = createTestConfig("dsbulk.engine", "executionId", "MyExecutionId");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.getCustomExecutionIdTemplate()).isEqualTo("MyExecutionId");
  }
}
