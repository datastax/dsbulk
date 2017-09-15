/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionUtils {
  private static final Config SHORTCUTS = ConfigFactory.parseResourcesAnySyntax("shortcuts.conf");
  private static final Logger LOGGER = LoggerFactory.getLogger(OptionUtils.class);

  // Maybe be overridden by Main, to handle the "-f" override for application.conf.
  public static Config DEFAULT = ConfigFactory.load().getConfig("dsbulk");

  public static Options createOptions(String connectorName) {
    Map<String, String> longToShortOptions = getLongToShortMap(connectorName);

    Options options = new Options();

    LoaderConfig config = new DefaultLoaderConfig(DEFAULT);

    for (Map.Entry<String, ConfigValue> entry : DEFAULT.entrySet()) {
      String longName = entry.getKey();
      Option option = createOption(config, longToShortOptions, longName, entry.getValue());
      options.addOption(option);
    }

    // Add the --help, --version, -f options
    options.addOption(
        null,
        "help",
        false,
        "This help text. May be combined with -c <connectorName> to see short options for a "
            + "particular connector");
    options.addOption(null, "version", false, "Print out the version of this tool.");
    options.addOption(SettingsDocumentor.CONFIG_FILE_OPTION);
    return options;
  }

  static Option createOption(
      LoaderConfig config,
      Map<String, String> longToShortOptions,
      String longName,
      ConfigValue value) {
    Option.Builder option;
    String shortName = longToShortOptions.get(longName);
    if (shortName == null) {
      option = Option.builder();
    } else {
      option = Option.builder(shortName);
    }
    option
        .hasArg()
        .longOpt(longName)
        .argName(config.getTypeString(longName))
        .desc(getSanitizedDescription(longName, value));
    return option.build();
  }

  @NotNull
  static Map<String, String> getLongToShortMap(String connectorName) {
    Map<String, String> longToShortOptions = new HashMap<>();

    // Add global shortcuts first
    for (Map.Entry<String, ConfigValue> entry :
        SHORTCUTS.getConfig("dsbulk.shortcuts").entrySet()) {
      longToShortOptions.put(entry.getValue().unwrapped().toString(), entry.getKey());
    }

    // Add connector-specific entries next. If there's overlap of shortcuts, log a warning.
    if (connectorName != null && SHORTCUTS.hasPath("dsbulk." + connectorName + "-shortcuts")) {
      for (Map.Entry<String, ConfigValue> entry :
          SHORTCUTS.getConfig("dsbulk." + connectorName + "-shortcuts").entrySet()) {
        String longOption = entry.getValue().unwrapped().toString();
        String shortOption = entry.getKey();
        if (longToShortOptions.containsKey(longOption)
            || longToShortOptions.containsValue(shortOption)) {
          LOGGER.warn(
              String.format(
                  "Shortcut %s => %s in %s shortcuts overlaps with global shortcuts and will be ignored",
                  shortOption, longOption, connectorName));
          continue;
        }
        longToShortOptions.put(longOption, shortOption);
      }
    }
    return longToShortOptions;
  }

  private static String getSanitizedDescription(String longName, ConfigValue value) {
    String desc =
        DEFAULT.getValue(longName).origin().comments().stream().collect(Collectors.joining("\n"));

    // The description is a little dirty.
    // * Replace consecutive spaces with a single space.
    // * Remove **'s, which have meaning in markdown but not useful here. However,
    //   we do have a legit case of ** when describing file patterns (e.g. **/*.csv).
    //   Those sorts of instances are preceded by ", so don't replace those.

    desc = desc.replaceAll(" +", " ").replaceAll("([^\"])\\*\\*", "$1").trim();
    desc += "\nDefaults to " + value.render(ConfigRenderOptions.concise()) + ".";
    return desc;
  }
}
