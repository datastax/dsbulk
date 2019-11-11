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

  /**
   * Invalidates caches and creates a resolved config containing only the driver settings that
   * DSBulk overrides.
   *
   * <p>The reference config is obtained from all classpath resources named driver-reference.conf.
   *
   * <p>This method is only useful for documentation purposes.
   *
   * @return a resolved reference config containing only the driver settings that DSBulk overrides.
   */
  public static Config standaloneDriverReference() {
    ConfigFactory.invalidateCaches();
    return ConfigFactory.parseResourcesAnySyntax("driver-reference").resolve();
  }

  /**
   * Invalidates caches and creates a resolved reference config for DSBulk.
   *
   * <p>The reference config is obtained from the following stack:
   *
   * <ol>
   *   <li>All classpath resources named dsbulk-reference.conf: DSBulk specific settings and driver
   *       overrides.
   *   <li>All classpath resources named dse-reference.conf: DSE driver specific settings.
   *   <li>All classpath resources named reference.conf: OSS driver settings.
   * </ol>
   *
   * @return a resolved reference config for DSBulk
   */
  @NonNull
  public static Config createReferenceConfig() {
    // parse errors should not happen here
    return ConfigFactory.parseResourcesAnySyntax("dsbulk-reference")
        .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
        .withFallback(ConfigFactory.defaultReference())
        .resolve();
  }

  /**
   * Invalidates caches and creates a resolved application config for DSBulk, optionally pulling
   * application settings from the given alternate location.
   *
   * <p>The application config is obtained from the following stack:
   *
   * <ol>
   *   <li>All classpath resources named application[.conf,.json,.properties] or <code>appConfigPath
   *       </code> if non null: application settings (DSBulk and driver overrides).
   *   <li>dsbulk-reference.conf: DSBulk specific settings and driver overrides.
   *   <li>dse-reference.conf: DSE driver specific settings.
   *   <li>reference.conf: OSS driver settings.
   * </ol>
   *
   * * @param appConfigPath An alternate location for the application settings, or null to use the
   * default application resources.
   *
   * @return a resolved application config for DSBulk
   */
  @NonNull
  public static Config createApplicationConfig(@Nullable Path appConfigPath) {
    try {
      if (appConfigPath != null) {
        // If the user specified the -f option (giving us an app config path),
        // set the config.file property to tell TypeSafeConfig.
        System.setProperty("config.file", appConfigPath.toString());
      }
      Config referenceConfig = createReferenceConfig();
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
