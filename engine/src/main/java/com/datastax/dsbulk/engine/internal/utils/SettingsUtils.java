/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.commons.config.LoaderConfig.LEAF_ANNOTATION;
import static com.datastax.dsbulk.commons.config.LoaderConfig.TYPE_ANNOTATION;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettingsUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsUtils.class);

  public static final LoaderConfig DEFAULT =
      new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk"));

  public static final Map<String, String> LONG_TO_SHORT_OPTIONS = new LinkedHashMap<>();

  public static final Option CONFIG_FILE_OPTION =
      Option.builder("f")
          .hasArg()
          .argName("string")
          .desc(
              "Load options from the given file rather than from `<dsbulk_home>/conf/application.conf`.")
          .build();

  public static final Map<String, Group> GROUPS =
      new TreeMap<>(new PriorityComparator("Common", "connector", "schema"));

  /** Common settings (both global and those from connectors). */
  static final List<String> COMMON_SETTINGS = new ArrayList<>();

  /**
   * Settings that should be placed near the top within their setting groups. It is a super-set of
   * COMMON_SETTINGS.
   */
  static final List<String> PREFERRED_SETTINGS;

  private static final Map<String, String> LONG_TO_SHORT_GLOBAL_OPTIONS = new LinkedHashMap<>();
  private static final Map<String, Map<String, String>> LONG_TO_SHORT_CONNECTOR_OPTIONS =
      new HashMap<>();

  static {
    // Add connector-specific common settings, after connector.name.
    COMMON_SETTINGS.add("connector.name");
    visitConnectors(
        (config, connectorName) -> {
          if (config.hasPath("metaSettings.docHints.commonSettings")) {
            config
                .getStringList("metaSettings.docHints.commonSettings")
                .stream()
                .map(s -> String.format("connector.%s.%s", connectorName, s))
                .forEach(COMMON_SETTINGS::add);
          }
        });
    COMMON_SETTINGS.addAll(DEFAULT.getStringList("metaSettings.docHints.commonSettings"));

    PREFERRED_SETTINGS = new ArrayList<>(COMMON_SETTINGS);
    PREFERRED_SETTINGS.addAll(DEFAULT.getStringList("metaSettings.docHints.preferredSettings"));

    // Add connector-specific preferred settings.
    visitConnectors(
        (config, connectorName) -> {
          if (config.hasPath("metaSettings.docHints.preferredSettings")) {
            config
                .getStringList("metaSettings.docHints.preferredSettings")
                .stream()
                .map(s -> String.format("connector.%s.%s", connectorName, s))
                .forEach(PREFERRED_SETTINGS::add);
          }
        });

    initLongToShortOptionsMaps();
    initGroups();
  }

  public static Map<String, String> getLongToShortOptionsMap(String connectorName) {
    // Add global shortcuts first
    Map<String, String> longToShortOptions = new HashMap<>(LONG_TO_SHORT_GLOBAL_OPTIONS);

    // Add connector-specific entries if available.
    Map<String, String> connectorLongToShortOptionsMap =
        LONG_TO_SHORT_CONNECTOR_OPTIONS.get(connectorName);
    if (connectorLongToShortOptionsMap == null) {
      return longToShortOptions;
    }

    connectorLongToShortOptionsMap.forEach(
        (longOption, shortOption) -> {
          if (longToShortOptions.containsKey(longOption)
              || longToShortOptions.containsValue(shortOption)) {
            LOGGER.warn(
                String.format(
                    "Shortcut %s => %s in %s shortcuts overlaps with global shortcuts and will be ignored",
                    shortOption, longOption, connectorName));
          } else {
            longToShortOptions.put(longOption, shortOption);
          }
        });
    return longToShortOptions;
  }

  private static void initLongToShortOptionsMaps() {
    // Process shortcut definitions in config to make a map of "long-setting" to "shortcut".

    // Start with global shortcuts.
    for (Map.Entry<String, ConfigValue> shortcutEntry :
        DEFAULT.getObject("metaSettings.shortcuts").entrySet()) {
      LONG_TO_SHORT_OPTIONS.put(
          shortcutEntry.getValue().unwrapped().toString(), shortcutEntry.getKey());
      LONG_TO_SHORT_GLOBAL_OPTIONS.put(
          shortcutEntry.getValue().unwrapped().toString(), shortcutEntry.getKey());
    }

    // Add on connector-specific shortcuts.
    visitConnectors(
        (config, connectorName) -> {
          if (config.hasPath("metaSettings.shortcuts")) {
            Map<String, String> connectorLongToShort = new LinkedHashMap<>();
            String pathPrefix = "connector." + connectorName + ".";
            for (Map.Entry<String, ConfigValue> shortcutEntry :
                config.getObject("metaSettings.shortcuts").entrySet()) {
              LONG_TO_SHORT_OPTIONS.put(
                  pathPrefix + shortcutEntry.getValue().unwrapped().toString(),
                  shortcutEntry.getKey());
              connectorLongToShort.put(
                  pathPrefix + shortcutEntry.getValue().unwrapped().toString(),
                  shortcutEntry.getKey());
            }
            LONG_TO_SHORT_CONNECTOR_OPTIONS.put(connectorName, connectorLongToShort);
          }
        });
  }

  private static void initGroups() {
    // First add a group for the "commonly used settings". Want to show that first in our
    // doc.
    GROUPS.put("Common", new FixedGroup());

    // Now add groups for every top-level setting section + driver.*.
    for (Map.Entry<String, ConfigValue> entry : DEFAULT.root().entrySet()) {
      String key = entry.getKey();
      if (key.equals("metaSettings")) {
        continue;
      }
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

    addSettings(DEFAULT.root(), "");
  }

  private static void addSettings(ConfigObject root, String keyPrefix) {
    for (Map.Entry<String, ConfigValue> entry : root.entrySet()) {

      String key = entry.getKey();

      // Never add a setting under "metaSettings".
      if (key.equals("metaSettings")) {
        continue;
      }

      ConfigValue value = entry.getValue();
      String fullKey = keyPrefix.isEmpty() ? key : keyPrefix + '.' + key;

      if (ConfigUtils.isLeaf(value)) {
        // Add the setting name to the first group that accepts it.
        for (Group group : GROUPS.values()) {
          if (group.addSetting(fullKey)) {
            // The setting was added to a group. Don't try adding to other groups.
            break;
          }
        }
      } else {
        addSettings((ConfigObject) value, fullKey);
      }
    }
  }

  /**
   * Walk through the setting sections for each connector and perform an action on the section.
   *
   * @param action the action to execute
   */
  private static void visitConnectors(BiConsumer<Config, String> action) {
    // Iterate through direct children of `connector`. Order is non-deterministic, so do
    // two passes: one to collect names of children and put in a sorted set, second to walk
    // through children in order.
    Set<String> connectorChildren = new TreeSet<>();
    for (Map.Entry<String, ConfigValue> nonLeafEntry : DEFAULT.getObject("connector").entrySet()) {
      connectorChildren.add(nonLeafEntry.getKey());
    }

    for (String connectorName : connectorChildren) {
      ConfigValue child = ConfigUtils.getNullSafeValue(DEFAULT, "connector." + connectorName);
      if (child.valueType() == ConfigValueType.OBJECT) {
        Config connectorConfig = ((ConfigObject) child).toConfig();
        action.accept(connectorConfig, connectorName);
      }
    }
  }

  public static boolean isAnnotation(String line) {
    return line.contains(TYPE_ANNOTATION) || line.contains(LEAF_ANNOTATION);
  }

  /**
   * Encapsulates a group of settings that should be rendered together. When adding a setting to a
   * group, the add may be rejected because the given setting doesn't fit the criteria of settings
   * that belong to the group. This allows groups to act as listeners for settings and accept only
   * those that they are interested in.
   */
  public interface Group {
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
      settings = new TreeSet<>(new PriorityComparator(PREFERRED_SETTINGS));
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
      desiredSettings = new HashSet<>(COMMON_SETTINGS);
      settings = new TreeSet<>(new PriorityComparator(COMMON_SETTINGS));
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
      Integer leftInd = Integer.MAX_VALUE;
      Integer rightInd = Integer.MAX_VALUE;
      for (String value : prioritizedValues.keySet()) {
        String valueWithDot = value + ".";
        if (left.startsWith(valueWithDot) || left.equals(value)) {
          leftInd = prioritizedValues.get(value);
        }
        if (right.startsWith(valueWithDot) || right.equals(value)) {
          rightInd = prioritizedValues.get(value);
        }
      }
      int indCompare = leftInd.compareTo(rightInd);
      return indCompare != 0 ? indCompare : left.compareTo(right);
    }
  }
}
