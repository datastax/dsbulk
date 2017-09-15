/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.commons.StringUtils;
import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.google.common.base.CharMatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.cli.Option;

public class SettingsDocumentor {
  private static final LoaderConfig DEFAULT =
      new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk"));
  private static final Map<String, String> LONG_TO_SHORT_OPTIONS;

  // NB: We can't use the Option Builder because Groovy locks us into
  // commons-cli v1.2, which has a different / incompatible option builder than
  // v1.4.
  static final Option CONFIG_FILE_OPTION =
      new Option(
          "f",
          null,
          true,
          "Load settings from the given file rather than `conf/application.conf`.");

  static {
    CONFIG_FILE_OPTION.setArgName("string");
  }

  /**
   * Settings that should be displayed in a "common" section as well as the appropriate place in the
   * hierarchy.
   */
  static final List<String> COMMON_SETTINGS =
      Arrays.asList(
          "connector.csv.url",
          "connector.name",
          "connector.csv.delimiter",
          "connector.csv.header",
          "connector.csv.skipLines",
          "connector.csv.maxLines",
          "schema.keyspace",
          "schema.table",
          "schema.mapping",
          "driver.hosts",
          "driver.port",
          "driver.auth.password",
          "driver.auth.username",
          "driver.query.consistency",
          "executor.maxPerSecond",
          "log.maxErrors",
          "log.directory",
          "monitoring.reportRate");

  /**
   * Settings that should be placed near the top within their setting groups. It is a super-set of
   * COMMON_SETTINGS.
   */
  static final List<String> PREFERRED_SETTINGS = new ArrayList<>(COMMON_SETTINGS);

  static final Map<String, Group> GROUPS =
      new TreeMap<>(new PriorityComparator("Common", "connector", "connector.csv", "schema"));

  static {
    PREFERRED_SETTINGS.add("driver.auth.provider");

    Config shortcutsConf =
        ConfigFactory.parseResourcesAnySyntax("shortcuts.conf").getConfig("dsbulk");
    LONG_TO_SHORT_OPTIONS = new HashMap<>();

    // Process shortcut definitions in config to make a map of "long-setting" to "shortcut".
    for (Map.Entry<String, ConfigValue> entry : shortcutsConf.root().entrySet()) {
      for (Map.Entry<String, ConfigValue> shortcutEntry :
          ((ConfigObject) entry.getValue()).entrySet()) {
        LONG_TO_SHORT_OPTIONS.put(
            shortcutEntry.getValue().unwrapped().toString(), shortcutEntry.getKey());
      }
    }

    initGroups();
  }

  public static void main(String[] args) {
    try {
      new SettingsDocumentor(args[0], Paths.get(args[1]));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("WeakerAccess")
  SettingsDocumentor(String title, Path filePath) throws FileNotFoundException {
    try (PrintWriter out =
        new PrintWriter(new BufferedOutputStream(new FileOutputStream(filePath.toFile())))) {
      // Print page title
      out.printf(
          "# %s%n%n"
              + "*NOTE:* The long options described here can be persisted in `conf/application.conf` "
              + "and thus permanently override defaults and avoid specifying options on the command "
              + "line.%n%n"
              + "## Sections%n%n",
          title);

      // Print links to relevant sections.
      for (String groupName : GROUPS.keySet()) {
        out.printf(
            "%s<a href=\"#%s\">%s Settings</a><br>%n",
            tocIndent(groupName), groupName, prettifyName(groupName));
      }

      // Walk through groups, emitting a group title followed by settings
      // for each group.
      for (Map.Entry<String, Group> groupEntry : GROUPS.entrySet()) {
        String groupName = groupEntry.getKey();
        out.printf("<a name=\"%s\"></a>%n", groupName);
        out.printf("%s %s Settings%n%n", titleFormat(groupName), prettifyName(groupName));
        if (!groupName.equals("Common")) {
          out.printf("%s%n%n", getSanitizedDescription(DEFAULT.getValue(groupName)));
        } else {
          // Emit the help for the "-f" option in the Common section.
          out.printf(
              "#### -f _&lt;%s&gt;_%n%n%s%n%n",
              StringUtils.htmlEscape("string"), CONFIG_FILE_OPTION.getDescription());
        }
        for (String settingName : groupEntry.getValue().getSettings()) {
          ConfigValue settingValue = DEFAULT.getValue(settingName);
          String shortOpt =
              LONG_TO_SHORT_OPTIONS.containsKey(settingName)
                  ? LONG_TO_SHORT_OPTIONS.get(settingName) + ","
                  : "";
          out.printf(
              "#### -%s--%s _&lt;%s&gt;_%n%n%s%n%n",
              shortOpt,
              settingName,
              StringUtils.htmlEscape(DEFAULT.getTypeString(settingName)),
              getSanitizedDescription(settingValue));
        }
      }
    }
  }

  private static void initGroups() {
    // First add a group for the "commonly used settings". Want to show that first in our
    // doc.
    GROUPS.put("Common", new FixedGroup());

    // Now add groups for every top-level setting section + driver.*.
    for (Map.Entry<String, ConfigValue> entry : DEFAULT.root().entrySet()) {
      String key = entry.getKey();
      if (key.equals("driver") || key.equals("connector")) {
        // "driver" group is special because it's really large;
        // "connector" group is special because we want a sub-section for each
        // particular connector.
        // We subdivide each one level further.
        GROUPS.put(key, new ContainerGroup(key, false));
        for (Map.Entry<String, ConfigValue> nonLeafEntry :
            ((ConfigObject) entry.getValue()).entrySet()) {
          if (nonLeafEntry.getValue().valueType() == ConfigValueType.OBJECT) {
            GROUPS.put(
                key + "." + nonLeafEntry.getKey(),
                new ContainerGroup(key + "." + nonLeafEntry.getKey(), true));
          }
        }
      } else {
        GROUPS.put(key, new ContainerGroup(key, true));
      }
    }

    for (Map.Entry<String, ConfigValue> entry : DEFAULT.entrySet()) {
      // Add the setting name to each group (and which groups want the setting will
      // take it).
      for (Group group : GROUPS.values()) {
        if (group.addSetting(entry.getKey())) {
          // The setting was added to a group. Don't try adding to other groups.
          break;
        }
      }
    }
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
        value
            .origin()
            .comments()
            .stream()
            .map(s -> s.length() > 0 ? s.substring(1) : s)
            .collect(Collectors.joining("\n"));
    if (value.valueType() != ConfigValueType.OBJECT) {
      desc +=
          String.format(
              "%n%nDefaults to **%s**.",
              value.render(ConfigRenderOptions.concise()).replace("*", "\\*"));
    }
    return desc;
  }

  /**
   * Encapsulates a group of settings that should be rendered together. When adding a setting to a
   * group, the add may be rejected because the given setting doesn't fit the criteria of settings
   * that belong to the group. This allows groups to act as listeners for settings and accept only
   * those that they are interested in.
   */
  interface Group {
    boolean addSetting(String settingName);

    Set<String> getSettings();
  }

  /**
   * A Group that contains settings at a particular level in the hierarchy, and possibly includes
   * all settings below that level.
   */
  static class ContainerGroup implements Group {
    private final Set<String> settings;
    private final String prefix;

    // Indicates whether this group should include settings that are descendants,
    // as opposed to just immediate children.
    private final boolean includeDescendants;

    ContainerGroup(String path, boolean includeDescendants) {
      this.prefix = path + ".";
      this.includeDescendants = includeDescendants;
      settings = new TreeSet<>(new PriorityComparator(SettingsDocumentor.PREFERRED_SETTINGS));
    }

    @Override
    public boolean addSetting(String settingName) {
      if (settingName.startsWith(prefix)
          && (includeDescendants || settingName.lastIndexOf(".") + 1 == prefix.length())) {
        settings.add(settingName);
      }
      return false;
    }

    @Override
    public Set<String> getSettings() {
      return settings;
    }
  }

  /** A group of particular settings. */
  static class FixedGroup implements Group {
    private final Set<String> desiredSettings;
    private final Set<String> settings;

    FixedGroup() {
      desiredSettings = new HashSet<>(SettingsDocumentor.COMMON_SETTINGS);
      settings = new TreeSet<>(new PriorityComparator(SettingsDocumentor.COMMON_SETTINGS));
    }

    @Override
    public boolean addSetting(String settingName) {
      if (desiredSettings.contains(settingName)) {
        settings.add(settingName);
      }

      // Always return false because we want other groups to have a chance to add this
      // setting as well.
      return false;
    }

    @Override
    public Set<String> getSettings() {
      return settings;
    }
  }

  /**
   * String comparator that supports placing "high priority" values first. This allows a setting
   * group to have "mostly" alpha-sorted settings, but with certain settings promoted to be first
   * (and thus emitted first when generating documentation).
   */
  static class PriorityComparator implements Comparator<String> {
    private final Map<String, Integer> prioritizedValues;

    PriorityComparator(String... highPriorityValues) {
      this(Arrays.asList(highPriorityValues));
    }

    PriorityComparator(List<String> highPriorityValues) {
      prioritizedValues = new HashMap<>();
      int counter = 0;
      for (String s : highPriorityValues) {
        prioritizedValues.put(s, counter++);
      }
    }

    @Override
    public int compare(String left, String right) {
      Integer leftInd = this.prioritizedValues.getOrDefault(left, Integer.MAX_VALUE);
      Integer rightInd = this.prioritizedValues.getOrDefault(right, Integer.MAX_VALUE);
      int indCompare = leftInd.compareTo(rightInd);
      return indCompare != 0 ? indCompare : left.compareTo(right);
    }
  }
}
