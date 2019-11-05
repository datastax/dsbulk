/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.config;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  public static Map<String, SettingsGroup> createGroups(@NonNull Config referenceConfig) {
    List<String> commonSettings = parseCommonSettings(referenceConfig);
    List<String> preferredSettings = parsePreferredSettings(referenceConfig, commonSettings);

    Map<String, SettingsGroup> groups = new TreeMap<>(POPULAR_SECTIONS_FIRST);

    // First add a group for the "commonly used settings". Want to show that first in our doc.
    groups.put("Common", new FixedSettingsGroup(commonSettings));

    // Now add groups for every top-level setting section in DSBulk config
    ConfigObject dsbulkRoot = referenceConfig.getConfig("dsbulk").root();
    addGroups(groups, dsbulkRoot, preferredSettings);
    // Now add settings to groups
    addSettings(groups, dsbulkRoot, "dsbulk");

    return groups;
  }

  @NonNull
  public static List<String> parseCommonSettings(@NonNull Config referenceConfig) {
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

  private static void addGroups(
      @NonNull Map<String, SettingsGroup> groups,
      @NonNull ConfigObject root,
      @NonNull List<String> preferredSettings) {
    for (Map.Entry<String, ConfigValue> entry : root.entrySet()) {
      String key = entry.getKey();
      if (key.equals("metaSettings")) {
        continue;
      }
      if (key.equals("connector") || key.equals("driver")) {
        // "connector" group is special because we want a sub-section for each
        // particular connector.
        // "driver" group is also special because it's really large.
        // We subdivide each one level further.
        groups.put(
            "dsbulk." + key, new ContainerSettingsGroup("dsbulk." + key, false, preferredSettings));
        for (Map.Entry<String, ConfigValue> nonLeafEntry :
            ((ConfigObject) entry.getValue()).entrySet()) {
          if (nonLeafEntry.getValue().valueType() == ConfigValueType.OBJECT) {
            groups.put(
                "dsbulk." + key + "." + nonLeafEntry.getKey(),
                new ContainerSettingsGroup(
                    "dsbulk." + key + "." + nonLeafEntry.getKey(), true, preferredSettings));
          }
        }
      } else {
        groups.put(
            "dsbulk." + key, new ContainerSettingsGroup("dsbulk." + key, true, preferredSettings));
      }
    }
  }

  private static void addSettings(
      @NonNull Map<String, SettingsGroup> groups,
      @NonNull ConfigObject root,
      @NonNull String keyPrefix) {
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
        for (SettingsGroup group : groups.values()) {
          if (group.addSetting(fullKey)) {
            // The setting was added to a group. Don't try adding to other groups.
            break;
          }
        }
      } else {
        addSettings(groups, (ConfigObject) value, fullKey);
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
