/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.Config;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class LoaderConfigFactoryTest {

  @Test
  void should_create_reference_config() {

    Config referenceConfig = LoaderConfigFactory.createReferenceConfig();
    assertReferenceConfig(referenceConfig);
  }

  @Test
  void should_create_application_config() {

    Config applicationConfig = LoaderConfigFactory.createApplicationConfig(null);

    assertReferenceConfig(applicationConfig);

    // should read application.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInApplication")).isTrue();
    assertThat(applicationConfig.getValue("dsbulk.definedInApplication").origin().description())
        .startsWith("application.conf @ file");
    // should not read application-custom.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInCustomApplication")).isFalse();
  }

  @Test
  void should_create_application_config_with_custom_location() throws URISyntaxException {

    Path appConfigPath = Paths.get(getClass().getResource("/application-custom.conf").toURI());
    Config applicationConfig = LoaderConfigFactory.createApplicationConfig(appConfigPath);

    assertReferenceConfig(applicationConfig);

    // should not read application.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInApplication")).isFalse();
    // should read application-custom.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInCustomApplication")).isTrue();
    assertThat(applicationConfig.getValue("dsbulk.definedInCustomApplication").origin().filename())
        .endsWith("application-custom.conf");
  }

  private static void assertReferenceConfig(Config referenceConfig) {
    // should read OSS driver reference.conf from its JAR
    assertThat(referenceConfig.hasPath("datastax-java-driver.basic.config-reload-interval"))
        .isTrue();
    assertThat(
            referenceConfig
                .getValue("datastax-java-driver.basic.config-reload-interval")
                .origin()
                .description())
        .startsWith("reference.conf @ jar");

    // should read DSE driver dse-reference.conf from its JAR
    assertThat(referenceConfig.hasPath("datastax-java-driver.basic.load-balancing-policy.class"))
        .isTrue();
    assertThat(
            referenceConfig
                .getValue("datastax-java-driver.basic.load-balancing-policy.class")
                .origin()
                .description())
        .startsWith("dse-reference.conf @ jar");

    // should read (dummy) dsbulk-reference.conf file found on the classpath
    assertThat(referenceConfig.hasPath("dsbulk.definedInDSBukReference")).isTrue();
    assertThat(referenceConfig.getValue("dsbulk.definedInDSBukReference").origin().description())
        .startsWith("dsbulk-reference.conf @ file");
  }
}
