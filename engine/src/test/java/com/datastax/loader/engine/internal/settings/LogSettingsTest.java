/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.loader.engine.internal.log.LogManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import org.junit.Before;
import org.junit.Test;

/** */
public class LogSettingsTest {

  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  public void should_create_log_manager_with_default_output_directory() throws Exception {
    Config config = ConfigFactory.load().getConfig("datastax-loader.log");
    LogSettings settings = new LogSettings(config, "test");
    LogManager logManager = settings.newLogManager();
    try {
      logManager.init(cluster);
      assertThat(logManager).isNotNull();
      assertThat(logManager.getOperationDirectory().toFile().getAbsolutePath())
          .isEqualTo(Paths.get("./test").normalize().toFile().getAbsolutePath());
    } finally {
      //noinspection ResultOfMethodCallIgnored
      Files.walk(logManager.getOperationDirectory())
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  public void should_create_log_manager_when_output_directory_url_provided() throws Exception {
    Path dir = Files.createTempDirectory("test");
    Config config =
        ConfigFactory.parseString("outputDirectory = \"" + dir.toUri().toURL() + "\"")
            .withFallback(ConfigFactory.load().getConfig("datastax-loader.log"));
    LogSettings settings = new LogSettings(config, "test");
    LogManager logManager = settings.newLogManager();
    logManager.init(cluster);
    assertThat(logManager).isNotNull();
    assertThat(logManager.getOperationDirectory().toFile()).isEqualTo(dir.resolve("test").toFile());
  }

  @Test
  public void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    Path dir = Files.createTempDirectory("test");
    Config config =
        ConfigFactory.parseString("output-directory = \"" + dir.toString() + "\"")
            .withFallback(ConfigFactory.load().getConfig("datastax-loader.log"));
    LogSettings settings = new LogSettings(config, "test");
    LogManager logManager = settings.newLogManager();
    logManager.init(cluster);
    assertThat(logManager).isNotNull();
    assertThat(logManager.getOperationDirectory().toFile()).isEqualTo(dir.resolve("test").toFile());
  }
}
