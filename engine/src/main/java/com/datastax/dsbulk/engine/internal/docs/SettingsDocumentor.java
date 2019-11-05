/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.docs;

import static com.datastax.dsbulk.engine.internal.help.HelpEntryFactory.CONFIG_FILE_OPTION;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.LoaderConfigFactory;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.internal.config.SettingsGroup;
import com.datastax.dsbulk.engine.internal.config.SettingsGroupFactory;
import com.datastax.dsbulk.engine.internal.config.ShortcutsFactory;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SettingsDocumentor {

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException(
          "Usage: ConfigurationFileCreator \"/path/to/destination/file\"");
    }
    Path filePath = Paths.get(args[0]);
    Files.createDirectories(filePath.getParent());
    try (PrintWriter out =
        new PrintWriter(
            Files.newBufferedWriter(
                filePath, StandardCharsets.UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
      // Print page title
      out.printf(
          "# %s%n%n"
              + "*NOTE:* The long options described here can be persisted in `conf/application.conf` "
              + "and thus permanently override defaults and avoid specifying options on the command "
              + "line.%n%n"
              + "A template configuration file can be found [here](./application.template.conf).%n%n"
              + "## Sections%n%n",
          WorkflowUtils.getBulkLoaderNameAndVersion() + " Options");

      Config referenceConfig = LoaderConfigFactory.createReferenceConfig();

      Map<String, SettingsGroup> groups = SettingsGroupFactory.createGroups(referenceConfig);
      Map<String, String> longToShortOptions = createLongToShortOptions(referenceConfig);

      // Print links to relevant sections.
      for (String groupName : groups.keySet()) {
        String noPrefix = removePrefix(groupName);
        out.printf(
            "%s<a href=\"#%s\">%s Settings</a><br>%n",
            tocIndent(noPrefix), noPrefix, prettifyName(noPrefix));
      }

      // Walk through groups, emitting a group title followed by settings
      // for each group.
      for (Map.Entry<String, SettingsGroup> groupEntry : groups.entrySet()) {
        String groupName = groupEntry.getKey();
        String noPrefix = removePrefix(groupName);
        out.printf("<a name=\"%s\"></a>%n", noPrefix);
        out.printf("%s %s Settings%n%n", titleFormat(noPrefix), prettifyName(noPrefix));
        if (!groupName.equals("Common")) {
          out.printf(
              "%s%n%n",
              getSanitizedDescription(ConfigUtils.getNullSafeValue(referenceConfig, groupName)));
        } else {
          // Emit the help for the "-f" option in the Common section.
          out.printf(
              "#### -f _&lt;%s&gt;_%n%n%s%n%n",
              StringUtils.htmlEscape("string"), CONFIG_FILE_OPTION.getDescription());
        }
        for (String settingName : groupEntry.getValue().getSettings()) {
          ConfigValue settingValue = ConfigUtils.getNullSafeValue(referenceConfig, settingName);
          String shortOpt =
              longToShortOptions.containsKey(settingName)
                  ? "-" + longToShortOptions.get(settingName) + ","
                  : "";
          out.printf(
              "#### %s--%s _&lt;%s&gt;_%n%n%s%n%n",
              shortOpt,
              settingName.replaceFirst("dsbulk\\.", "[dsbulk.]"),
              StringUtils.htmlEscape(
                  ConfigUtils.getTypeString(referenceConfig, settingName).orElse("arg")),
              getSanitizedDescription(settingValue));
        }
      }
    }
  }

  /** Collect shortcuts for all known connectors. */
  private static Map<String, String> createLongToShortOptions(Config referenceConfig) {
    Map<String, String> longToShortOptions = new HashMap<>();
    for (String connectorName : referenceConfig.getConfig("dsbulk.connector").root().keySet()) {
      if ("name".equals(connectorName)) {
        continue;
      }
      longToShortOptions.putAll(
          ShortcutsFactory.createShortcutsMap(referenceConfig, connectorName).inverse());
    }
    return longToShortOptions;
  }

  /**
   * When emitting a link to a group in the toc section, emit it based on how nested the group
   * specification is (e.g. driver.auth will be indented more than driver).
   *
   * @param groupName Name of settings group
   * @return As many non-breaking-whitespaces as is needed for this group.
   */
  private static String tocIndent(String groupName) {
    return StringUtils.nCopies("&nbsp;&nbsp;&nbsp;", CharMatcher.is('.').countIn(groupName));
  }

  /**
   * When emitting a section title (just before the relevant settings), format its font size based
   * on its nesting (e.g. driver.auth may be an h3, while driver may be an h2).
   *
   * @param groupName Name of settings group
   * @return format string (markdown headers)
   */
  private static String titleFormat(String groupName) {
    return StringUtils.nCopies("#", CharMatcher.is('.').countIn(groupName) + 2);
  }

  /**
   * Convert the group name to a prettier representation (e.g. driver.auth => Driver Auth).
   *
   * @param groupName Name of settings group
   * @return pretty representation of the group name.
   */
  private static String prettifyName(String groupName) {
    return Arrays.stream(groupName.split("\\."))
        .map(StringUtils::ucfirst)
        .collect(Collectors.joining(" "));
  }

  /**
   * Process the comment of a particular setting to produce markdown for rendering in the doc.
   *
   * @param value ConfigValue object for the desired setting.
   * @return markdown string.
   */
  private static String getSanitizedDescription(ConfigValue value) {
    // We collect all the lines in the comment block for the setting and join with newlines.
    // However, each line starts with a single leading space that we want to remove.
    String desc =
        value.origin().comments().stream()
            .filter(line -> !ConfigUtils.isTypeHint(line))
            .filter(line -> !ConfigUtils.isLeaf(line))
            .map(s -> s.length() > 0 ? s.substring(1) : s)
            .collect(Collectors.joining("\n"));
    if (value.valueType() != ConfigValueType.OBJECT) {
      String defaultValue = value.render(ConfigRenderOptions.concise()).replace("*", "\\*");
      if (defaultValue.equals("\"\"")) {
        defaultValue = "&lt;unspecified&gt;";
      }
      desc += String.format("%n%nDefault: **%s**.", defaultValue);
    }
    return desc;
  }

  private static String removePrefix(String s) {
    return s.replaceFirst(".*?\\.", "");
  }
}
