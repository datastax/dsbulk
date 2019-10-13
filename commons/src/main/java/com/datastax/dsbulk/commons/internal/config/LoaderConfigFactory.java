/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Path;

public class LoaderConfigFactory {

  @NonNull
  public static Config createReferenceConfig() {
    // parse errors should not happen here
    return ConfigFactory.parseResourcesAnySyntax("dsbulk-reference")
        .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
        .withFallback(ConfigFactory.defaultReference())
        .resolve();
  }

  @NonNull
  public static Config createApplicationConfig(
      @Nullable Path appConfigPath, @NonNull Config referenceConfig) {
    try {
      if (appConfigPath != null) {
        // If the user specified the -f option (giving us an app config path),
        // set the config.file property to tell TypeSafeConfig.
        System.setProperty("config.file", appConfigPath.toString());
      }
      return ConfigFactory.defaultOverrides()
          .withFallback(ConfigFactory.defaultApplication())
          .withFallback(referenceConfig)
          .resolve();
    } catch (ConfigException.Parse e) {
      throw new IllegalArgumentException(
          String.format(
              "Error parsing configuration file %s at line %s. "
                  + "Please make sure its format is compliant with HOCON syntax. "
                  + "If you are using \\ (backslash) to define a path, "
                  + "escape it with \\\\ or use / (forward slash) instead.",
              e.origin().filename(), e.origin().lineNumber()),
          e);
    }
  }
}
