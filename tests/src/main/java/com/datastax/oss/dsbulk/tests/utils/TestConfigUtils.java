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
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import java.util.Arrays;
import java.util.Iterator;

public class TestConfigUtils {

  /**
   * Creates a test configuration based on the regular application + reference config, and using the
   * provided additional arguments to emulate command line arguments.
   *
   * @param path The root path of the configuration.
   * @param additionalArgs Additional arguments to be emulated as command line arguments.
   * @return A test configuration containing all the settings under the section 'path'.
   */
  public static Config createTestConfig(String path, Object... additionalArgs) {
    Config baseConfig = ConfigUtils.createApplicationConfig(null).getConfig(path);
    if (additionalArgs != null && additionalArgs.length != 0) {
      Iterator<Object> it = Arrays.asList(additionalArgs).iterator();
      while (it.hasNext()) {
        Object key = it.next();
        Object value = it.next();
        baseConfig =
            ConfigFactory.parseString(
                    key + "=" + value,
                    ConfigParseOptions.defaults().setOriginDescription("command line argument"))
                .withFallback(baseConfig);
      }
    }
    return baseConfig;
  }
}
