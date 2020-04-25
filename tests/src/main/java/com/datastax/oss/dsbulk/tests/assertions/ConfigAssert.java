/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
