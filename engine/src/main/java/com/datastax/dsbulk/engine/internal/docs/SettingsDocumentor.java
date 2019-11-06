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
import static java.nio.charset.StandardCharsets.UTF_8;
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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
            Files.newBufferedWriter(filePath, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
      Config referenceConfig = LoaderConfigFactory.createReferenceConfig();
      Map<String, SettingsGroup> groups =
          SettingsGroupFactory.createDSBulkConfigurationGroups(true);
      SettingsGroup driverGroup = groups.remove("datastax-java-driver");
      Map<String, String> longToShortOptions = createLongToShortOptions(referenceConfig);
      printTitle(out);
      printLinks(out, groups);
      printDsbulkSections(out, referenceConfig, groups, longToShortOptions);
      printDriverSection(out, referenceConfig, driverGroup, longToShortOptions);
    }
  }

  private static void printTitle(PrintWriter out) {
    out.printf(
        "# %s%n%n"
            + "*NOTE:* The long options described here can be persisted in `conf/application.conf` "
            + "and thus permanently override defaults and avoid specifying options on the command "
            + "line.%n%n"
            + "A template configuration file can be found [here](./application.template.conf).%n%n"
            + "## Sections%n%n",
        WorkflowUtils.getBulkLoaderNameAndVersion() + " Options");
  }

  private static void printLinks(PrintWriter out, Map<String, SettingsGroup> groups) {
    // Print links to relevant sections.
    for (String groupName : groups.keySet()) {
      String noPrefix = removePrefix(groupName);
      out.printf(
          "%s<a href=\"#%s\">%s</a><br>%n", tocIndent(noPrefix), noPrefix, prettifyName(noPrefix));
    }
    // link to driver section
    out.printf(
        "%s<a href=\"#%s\">%s</a><br>%n", tocIndent(""), "datastax-java-driver", "Driver Settings");
  }

  private static void printDsbulkSections(
      PrintWriter out,
      Config referenceConfig,
      Map<String, SettingsGroup> groups,
      Map<String, String> longToShortOptions) {
    // Walk through groups, emitting a group title followed by settings
    // for each group.
    for (Entry<String, SettingsGroup> groupEntry : groups.entrySet()) {
      String groupName = groupEntry.getKey();
      String noPrefix = removePrefix(groupName);
      out.printf("<a name=\"%s\"></a>%n", noPrefix);
      out.printf("%s %s%n%n", titleFormat(noPrefix), prettifyName(noPrefix));
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
        if (settingName.startsWith("dsbulk.")) {
          printDsbulkSetting(out, referenceConfig, longToShortOptions, settingName, settingValue);
        } else {
          printDriverSetting(out, referenceConfig, longToShortOptions, settingName, settingValue);
        }
      }
    }
  }

  private static void printDriverSection(
      @NonNull PrintWriter out,
      @NonNull Config referenceConfig,
      @NonNull SettingsGroup driverGroup,
      @NonNull Map<String, String> longToShortOptions) {
    out.println("<a name=\"datastax-java-driver\"></a>");
    out.println("## Driver Settings");
    out.println();
    out.println(
        "The settings below are just a subset of all the configurable options of the driver, "
            + "and provide an optimal driver configuration for DSBulk for most use cases.");
    out.println();
    out.println(
        "See the [Java Driver configuration reference](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration) "
            + "for instructions on how to configure the driver properly.");
    out.println();
    out.println(
        "Note: driver settings always start with prefix `datastax-java-driver`; on the command line only, "
            + "it is possible to abbreviate this prefix to just `driver`, as shown below.");
    out.println();
    for (String settingName : driverGroup.getSettings()) {
      ConfigValue settingValue = ConfigUtils.getNullSafeValue(referenceConfig, settingName);
      printDriverSetting(out, referenceConfig, longToShortOptions, settingName, settingValue);
    }
  }

  private static void printDsbulkSetting(
      @NonNull PrintWriter out,
      @NonNull Config referenceConfig,
      @NonNull Map<String, String> longToShortOptions,
      @NonNull String settingName,
      @NonNull ConfigValue settingValue) {
    String shortOpt =
        longToShortOptions.containsKey(settingName)
            ? "-" + longToShortOptions.get(settingName) + ",<br />"
            : "";
    out.printf(
        "#### %s--%s _&lt;%s&gt;_%n%n%s%n%n",
        shortOpt,
        settingName.replaceFirst("dsbulk\\.", "[dsbulk.]"),
        StringUtils.htmlEscape(
            ConfigUtils.getTypeString(referenceConfig, settingName).orElse("arg")),
        getSanitizedDescription(settingValue));
  }

  private static void printDriverSetting(
      @NonNull PrintWriter out,
      @NonNull Config referenceConfig,
      @NonNull Map<String, String> longToShortOptions,
      @NonNull String settingName,
      @NonNull ConfigValue settingValue) {
    String shortOpt =
        longToShortOptions.containsKey(settingName)
            ? "-" + longToShortOptions.get(settingName) + ",<br />"
            : "";
    String abbreviatedLongOpt = settingName.replace("datastax-java-driver.", "driver.");
    out.printf(
        "#### %s--%s<br />--%s _&lt;%s&gt;_%n%n%s%n%n",
        shortOpt,
        abbreviatedLongOpt,
        settingName,
        StringUtils.htmlEscape(
            ConfigUtils.getTypeString(referenceConfig, settingName).orElse("arg")),
        getSanitizedDescription(settingValue));
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
    String title =
        Arrays.stream(groupName.split("\\."))
                .map(StringUtils::ucfirst)
                .collect(Collectors.joining(" "))
            + " Settings";
    if (title.contains("Driver")) {
      title += " (Deprecated)";
    }
    return title;
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
