/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.commons.config.model;

import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

public class SettingsGroupFactory {

  /**
   * A String comparator that places the following 3 sections on top of generated documentation:
   *
   * <ol>
   *   <li>Common settings
   *   <li>Connector settings
   *   <li>Schema settings
   * </ol>
   */
  private static final SettingsComparator POPULAR_SECTIONS_FIRST =
      new SettingsComparator("Common", "dsbulk.connector", "dsbulk.schema");

  /**
   * A comparator for configuration entries that preserves the original order in which they are
   * found in the configuration file. Used for the driver section only.
   */
  public static final Comparator<Entry<String, ConfigValue>> ORIGIN_COMPARATOR =
      (e1, e2) -> {
        if (e1.getKey().equals(e2.getKey())) {
          return 0;
        }
        ConfigOrigin o1 = e1.getValue().origin();
        ConfigOrigin o2 = e2.getValue().origin();
        int positionComparison = Integer.compare(o1.lineNumber(), o2.lineNumber());
        if (positionComparison != 0) {
          return positionComparison;
        }
        // compare keys alphabetically
        return e1.getKey().compareTo(e2.getKey());
      };

  /**
   * Creates the configuration sections for DSBulk. The sections created here are used by the help
   * emitter in dsbulk-runner module, and by documentation generation tools found in dsbulk-docs
   * module.
   *
   * @param includeDriver Whether to include a driver section or not.
   */
  public static Map<String, SettingsGroup> createDSBulkConfigurationGroups(boolean includeDriver) {
    Config referenceConfig = ConfigUtils.standaloneDSBulkReference();
    List<String> commonSettings = parseCommonSettings(referenceConfig);
    List<String> preferredSettings = parsePreferredSettings(referenceConfig, commonSettings);
    SettingsComparator comparator = new SettingsComparator(preferredSettings);
    Map<String, SettingsGroup> groups = new TreeMap<>(POPULAR_SECTIONS_FIRST);

    // First add a group for the "commonly used settings". Want to show that first in our doc.
    groups.put("Common", new FixedSettingsGroup(commonSettings));

    // Now add groups for every top-level setting section in DSBulk config
    ConfigObject dsbulkRoot = referenceConfig.getConfig("dsbulk").root();
    addDsbulkSections(groups, dsbulkRoot, comparator);
    // Now add settings to DSBulk groups
    populateDsbulkSettings(groups, dsbulkRoot, "dsbulk");

    if (includeDriver) {
      Config driverConfig =
          ConfigUtils.standaloneDriverReference().getConfig("datastax-java-driver");
      SettingsGroup driverGroup = new OrderedSettingsGroup();
      populateDriverGroup(driverGroup, driverConfig.root(), "datastax-java-driver");
      groups.put("datastax-java-driver", driverGroup);
    }
    return groups;
  }

  @NonNull
  private static List<String> parseCommonSettings(@NonNull Config referenceConfig) {
    // Common settings (both global and those from connectors).
    List<String> commonSettings = new ArrayList<>();
    // Add connector-specific common settings, after connector.name.
    commonSettings.add("dsbulk.connector.name");
    visitConnectors(
        referenceConfig,
        (connectorName, connectorConfig) -> {
          if (connectorConfig.hasPath("metaSettings.docHints.commonSettings")) {
            connectorConfig.getStringList("metaSettings.docHints.commonSettings").stream()
                // connector common settings are unqualified
                .map(s -> String.format("dsbulk.connector.%s.%s", connectorName, s))
                .forEach(commonSettings::add);
          }
        });
    // DSBulk general common settings are already fully-qualified
    commonSettings.addAll(
        referenceConfig.getStringList("dsbulk.metaSettings.docHints.commonSettings"));
    return commonSettings;
  }

  @NonNull
  private static List<String> parsePreferredSettings(
      @NonNull Config referenceConfig, @NonNull List<String> commonSettings) {
    // Settings that should be placed near the top within their setting groups. It is a super-set of
    // commonSettings.
    List<String> preferredSettings = new ArrayList<>(commonSettings);
    preferredSettings.addAll(
        referenceConfig.getStringList("dsbulk.metaSettings.docHints.preferredSettings"));
    // Add connector-specific preferred settings.
    visitConnectors(
        referenceConfig,
        (connectorName, connectorConfig) -> {
          if (connectorConfig.hasPath("metaSettings.docHints.preferredSettings")) {
            connectorConfig.getStringList("metaSettings.docHints.preferredSettings").stream()
                .map(s -> String.format("dsbulk.connector.%s.%s", connectorName, s))
                .forEach(preferredSettings::add);
          }
        });
    return preferredSettings;
  }

  private static void addDsbulkSections(
      @NonNull Map<String, SettingsGroup> groups,
      @NonNull ConfigObject root,
      @NonNull Comparator<String> comparator) {
    for (Map.Entry<String, ConfigValue> entry : root.entrySet()) {
      String key = entry.getKey();
      switch (key) {
        case "metaSettings":
          // never print meta-settings
          break;
        case "driver":
          // deprecated as of 1.4.0
          break;
        case "connector":
          // "connector" group is special because we want a sub-section for each
          // particular connector. We subdivide each one level further.
          groups.put(
              "dsbulk." + key, new ContainerSettingsGroup("dsbulk." + key, false, comparator));
          for (Map.Entry<String, ConfigValue> child :
              ((ConfigObject) entry.getValue()).entrySet()) {
            if (!ConfigUtils.isLeaf(child.getValue())) {
              groups.put(
                  "dsbulk." + key + "." + child.getKey(),
                  new ContainerSettingsGroup(
                      "dsbulk." + key + "." + child.getKey(), true, comparator));
            }
          }
          break;
        default:
          groups.put(
              "dsbulk." + key, new ContainerSettingsGroup("dsbulk." + key, true, comparator));
          break;
      }
    }
  }

  private static void populateDsbulkSettings(
      @NonNull Map<String, SettingsGroup> groups,
      @NonNull ConfigObject root,
      @NonNull String rootPath) {
    for (Map.Entry<String, ConfigValue> entry : root.entrySet()) {
      String key = entry.getKey();
      // Never add a setting under "metaSettings".
      if (key.equals("metaSettings")) {
        continue;
      }
      ConfigValue value = entry.getValue();
      String fullKey = rootPath.isEmpty() ? key : rootPath + '.' + key;
      if (ConfigUtils.isLeaf(value)) {
        // Add the setting name to the first group that accepts it.
        for (SettingsGroup group : groups.values()) {
          if (group.addSetting(fullKey)) {
            // The setting was added to a group. Don't try adding to other groups.
            break;
          }
        }
      } else {
        populateDsbulkSettings(groups, (ConfigObject) value, fullKey);
      }
    }
  }

  private static void populateDriverGroup(
      @NonNull SettingsGroup driverGroup, @NonNull ConfigObject root, @NonNull String path) {
    Set<Entry<String, ConfigValue>> entries = new TreeSet<>(ORIGIN_COMPARATOR);
    entries.addAll(root.entrySet());
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      String fullKey = path.isEmpty() ? key : path + '.' + key;
      ConfigValue value = ConfigUtils.getNullSafeValue(root.toConfig(), key);
      if (ConfigUtils.isLeaf(value)) {
        driverGroup.addSetting(fullKey);
      } else {
        populateDriverGroup(driverGroup, ((ConfigObject) value), fullKey);
      }
    }
  }

  /**
   * Walk through the setting sections for each connector and perform an action on the section.
   *
   * @param action the action to execute
   */
  private static void visitConnectors(Config referenceConfig, BiConsumer<String, Config> action) {
    // Iterate through direct children of `connector`. Order is non-deterministic, so do
    // two passes: one to collect names of children and put in a sorted set, second to walk
    // through children in order.
    Set<String> connectorChildren = new TreeSet<>();
    for (Map.Entry<String, ConfigValue> nonLeafEntry :
        referenceConfig.getObject("dsbulk.connector").entrySet()) {
      connectorChildren.add(nonLeafEntry.getKey());
    }
    for (String connectorName : connectorChildren) {
      ConfigValue child =
          ConfigUtils.getNullSafeValue(referenceConfig, "dsbulk.connector." + connectorName);
      if (child.valueType() == ConfigValueType.OBJECT) {
        Config connectorConfig = ((ConfigObject) child).toConfig();
        action.accept(connectorName, connectorConfig);
      }
    }
  }
}
