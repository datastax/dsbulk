/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.help;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HelpEntryFactory {

  public static final HelpEntry CONFIG_FILE_OPTION =
      new HelpEntry(
          "f",
          null,
          "string",
          "Load options from the given file rather than from `<dsbulk_home>/conf/application.conf`.");

  public static final HelpEntry HELP_OPTION =
      new HelpEntry(
          null,
          "help",
          null,
          "This help text. May be combined with -c <connectorName> to see short options for a "
              + "particular connector.");

  public static final HelpEntry VERSION_OPTION =
      new HelpEntry("v", "version", null, "Show program's version number and exit.");

  public static List<HelpEntry> createEntries(
      Collection<String> settings, Map<String, String> longToShortOptions, Config referenceConfig) {
    List<HelpEntry> entries = new ArrayList<>();
    for (String setting : settings) {
      String argumentType = ConfigUtils.getTypeString(referenceConfig, setting).orElse("arg");
      ConfigValue value = ConfigUtils.getNullSafeValue(referenceConfig, setting);
      HelpEntry entry =
          new HelpEntry(
              longToShortOptions.get(setting),
              createLongOptionName(setting),
              argumentType,
              getSanitizedDescription(value));
      entries.add(entry);
    }
    return entries;
  }

  private static String createLongOptionName(String setting) {
    return setting
        // the prefix "dsbulk." is optional
        .replaceFirst("dsbulk\\.", "[dsbulk.]");
  }

  /**
   * Adapts the configuration value comments and makes it suitable for rendering on the console.
   *
   * <p>TODO replace this with a proper Markdown render tool.
   *
   * @param value The configuration value to sanitize.
   * @return A sanitized description suitable for rendering on the console.
   */
  private static String getSanitizedDescription(ConfigValue value) {
    String desc = ConfigUtils.getComments(value);
    desc =
        desc
            // * Replace consecutive spaces with a single space.
            .replaceAll(" +", " ")
            // * Remove **'s, which have meaning in markdown but not useful here. However,
            //   we do have a legit case of ** when describing file patterns (e.g. **/*.csv).
            //   Those sorts of instances are preceded by ", so don't replace those.
            .replaceAll("([^\"])\\*\\*", "$1")
            // * Replace ``` with empty string
            .replaceAll("```\n", "")
            // * Replace ` with '
            .replaceAll("`", "'")
            .trim();
    String defaultValue = value.render(ConfigRenderOptions.concise());
    desc += "\nDefault: " + defaultValue;
    return desc;
  }
}
