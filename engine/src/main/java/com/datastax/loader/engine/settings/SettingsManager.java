/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SettingsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsManager.class);

  private static final Config REFERENCE =
      ConfigFactory.defaultReference().getConfig("datastax-loader");

  public static final Config DEFAULT = ConfigFactory.load().getConfig("datastax-loader");

  public Config loadConfig(String[] args) {
    Config config = parseUserSettings(args).withFallback(DEFAULT);
    config.checkValid(REFERENCE);
    // TODO check unrecognized config
    ConfigRenderOptions renderOptions =
        ConfigRenderOptions.defaults().setJson(false).setOriginComments(false).setComments(false);
    LOGGER.info("DataStax Loader settings: \n" + config.root().render(renderOptions));
    return config;
  }

  private static Config parseUserSettings(String[] args) {
    Config userSettings = ConfigFactory.empty();
    for (String arg : args) {
      userSettings = ConfigFactory.parseString(arg).withFallback(userSettings);
    }
    return userSettings;
  }
}
