/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ConfigurationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);

  public Config loadConfig(String[] args) {
    Config config = ConfigFactory.load();
    config.checkValid(ConfigFactory.defaultReference(), "datastax-loader");
    config = config.getConfig("datastax-loader");
    Map<String, String> userSettings = new HashMap<>();
    for (String arg : args) {
      String[] tokens = arg.split("=");
      userSettings.put(tokens[0], tokens[1]);
    }
    Config userConfig = ConfigFactory.parseMap(userSettings, "user-supplied settings");
    config = userConfig.withFallback(config);
    ConfigRenderOptions renderOptions =
        ConfigRenderOptions.defaults().setJson(false).setOriginComments(false).setComments(false);
    LOGGER.info("DataStax Loader configuration: \n" + config.root().render(renderOptions));
    return config;
  }
}
