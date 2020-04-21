/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
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
