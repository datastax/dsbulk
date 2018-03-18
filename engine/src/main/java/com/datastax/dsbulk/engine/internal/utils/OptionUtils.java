/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.CONFIG_FILE_OPTION;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OptionUtils {
  public static Options createOptions(String connectorName) {
    Map<String, String> longToShortOptions = SettingsUtils.getLongToShortOptionsMap(connectorName);

    Options options = new Options();

    for (Map.Entry<String, ConfigValue> entry : DataStaxBulkLoader.DEFAULT.entrySet()) {
      String longName = entry.getKey();
      Option option =
          createOption(DataStaxBulkLoader.DEFAULT, longToShortOptions, longName, entry.getValue());
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
    options.addOption(CONFIG_FILE_OPTION);
    return options;
  }

  static Option createOption(
      Config config, Map<String, String> longToShortOptions, String longName, ConfigValue value) {
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
        .argName(ConfigUtils.getTypeString(config, longName))
        .desc(getSanitizedDescription(longName, value));
    return option.build();
  }

  private static String getSanitizedDescription(String longName, ConfigValue value) {
    String desc =
        DataStaxBulkLoader.DEFAULT
            .getValue(longName)
            .origin()
            .comments()
            .stream()
            .collect(Collectors.joining("\n"));

    // The description is a little dirty.
    // * Replace consecutive spaces with a single space.
    // * Remove **'s, which have meaning in markdown but not useful here. However,
    //   we do have a legit case of ** when describing file patterns (e.g. **/*.csv).
    //   Those sorts of instances are preceded by ", so don't replace those.
    // * Replace ` with '

    desc = desc.replaceAll(" +", " ").replaceAll("([^\"])\\*\\*", "$1").replaceAll("`", "'").trim();
    String defaultValue = value.render(ConfigRenderOptions.concise());
    if (defaultValue.equals("\"\"")) {
      defaultValue = "<unspecified>";
    }
    desc += "\nDefault: " + defaultValue;
    return desc;
  }
}
