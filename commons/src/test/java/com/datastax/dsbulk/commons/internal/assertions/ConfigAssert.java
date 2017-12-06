/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.assertions;

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
