/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.typesafe.config.Config;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** */
public class SettingsManagerTest {

  @Test
  public void should_create_config_without_user_settings() throws Exception {
    SettingsManager settingsManager = new SettingsManager();
    String[] args = {};
    Config config = settingsManager.loadConfig(args);
    assertThat(config).isNotNull();
    assertThat(config.hasPath("connector.class")).isFalse();
  }

  @Test
  public void should_create_config_with_user_settings() throws Exception {
    SettingsManager settingsManager = new SettingsManager();
    String[] args = {
      "connector.class=com.datastax.loader.connectors.csv.CSVConnector",
      "batch.mode=SORTED",
      "schema.mapping={0=c2,2=c1}"
    };
    Config config = settingsManager.loadConfig(args);
    assertThat(config).isNotNull();
    assertThat(config.hasPath("connector.class")).isTrue();
    assertThat(config.getString("connector.class"))
        .isEqualTo("com.datastax.loader.connectors.csv.CSVConnector");
    assertThat(config.hasPath("batch.mode")).isTrue();
    assertThat(config.getString("batch.mode")).isEqualTo("SORTED");
    assertThat(config.hasPath("connector.class")).isTrue();
    assertThat(config.getObject("schema.mapping").unwrapped())
        .containsOnlyKeys("0", "2")
        .containsValue("c1")
        .containsValue("c2");
  }
}
