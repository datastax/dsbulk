/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.Config;
import org.assertj.core.api.AbstractObjectAssert;

@SuppressWarnings("UnusedReturnValue")
public class ConfigAssert extends AbstractObjectAssert<ConfigAssert, Config> {

  ConfigAssert(Config config) {
    super(config, ConfigAssert.class);
  }

  private ConfigAssert hasPath(String path) {
    assertThat(actual.hasPath(path))
        .overridingErrorMessage("Expecting %s to have path %s but it did not", actual, path)
        .isTrue();
    return this;
  }

  public ConfigAssert doesNotHavePath(String path) {
    assertThat(actual.hasPath(path))
        .overridingErrorMessage("Expecting %s to not have path %s but it did", actual, path)
        .isFalse();
    return this;
  }

  public ConfigAssert hasPaths(String... paths) {
    for (String path : paths) {
      hasPath(path);
    }
    return this;
  }
}
