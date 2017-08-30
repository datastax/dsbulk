/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.typesafe.config.Config;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;

@SuppressWarnings("UnusedReturnValue")
public class ConfigAssert extends ObjectAssert<Config> {

  public ConfigAssert(Config config) {
    super(config);
  }

  public ConfigAssert hasPath(String path) {
    Assertions.assertThat(actual.hasPath(path))
        .as("Expecting %s to have path %s but it did not", actual, path)
        .isTrue();
    return this;
  }

  public ConfigAssert doesNotHavePath(String path) {
    Assertions.assertThat(actual.hasPath(path))
        .as("Expecting %s to not have path %s but it did", actual, path)
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
