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
package com.datastax.oss.dsbulk.config.shortcuts;

import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableBiMap;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShortcutsFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShortcutsFactory.class);

  /**
   * Creates a BiMap where keys are shortcuts and values are settings, using the default reference
   * config and not favoring any particular connector.
   *
   * @return a BiMap where keys are shortcuts and values are settings.
   */
  @NonNull
  public static BiMap<String, String> createShortcutsMap() {
    return createShortcutsMap(ConfigUtils.standaloneDSBulkReference(), null);
  }

  /**
   * Creates a BiMap where keys are shortcuts and values are settings.
   *
   * @param referenceConfig The reference config to use.
   * @param connectorName The connector name to use for creating connector shortcuts.
   * @return a BiMap where keys are shortcuts and values are settings.
   */
  @NonNull
  public static BiMap<String, String> createShortcutsMap(
      @NonNull Config referenceConfig, @Nullable String connectorName) {
    Map<String, String> shortcutsMap = new HashMap<>();
    addGlobalShortcuts(referenceConfig, shortcutsMap);
    if (connectorName != null) {
      addConnectorShortcuts(referenceConfig, connectorName, shortcutsMap);
    }
    return ImmutableBiMap.copyOf(shortcutsMap);
  }

  private static void addGlobalShortcuts(
      @NonNull Config referenceConfig, @NonNull Map<String, String> shortcutsMap) {
    // global shortcuts
    ConfigObject globalShortcutsConfig = referenceConfig.getObject("dsbulk.metaSettings.shortcuts");
    for (Map.Entry<String, ConfigValue> shortcutEntry : globalShortcutsConfig.entrySet()) {
      String shortcut = shortcutEntry.getKey();
      // global long options are expected to be qualified (i.e. they should start with "dsbulk." or
      // "datastax-java-driver.")
      String longOption = shortcutEntry.getValue().unwrapped().toString();
      shortcutsMap.put(shortcut, longOption);
    }
  }

  private static void addConnectorShortcuts(
      @NonNull Config referenceConfig,
      @NonNull String connectorName,
      @NonNull Map<String, String> shortcutsMap) {
    String connectorShortcutsPath =
        String.format("dsbulk.connector.%s.metaSettings.shortcuts", connectorName);
    if (referenceConfig.hasPath(connectorShortcutsPath)) {
      ConfigObject connectorShortcutsConfig = referenceConfig.getObject(connectorShortcutsPath);
      for (Map.Entry<String, ConfigValue> shortcutEntry : connectorShortcutsConfig.entrySet()) {
        String shortcut = shortcutEntry.getKey();
        // connector-specific targets are expected to be unqualified, so qualify them now
        String target =
            "dsbulk.connector." + connectorName + '.' + shortcutEntry.getValue().unwrapped();
        if (shortcutsMap.putIfAbsent(shortcut, target) != null) {
          LOGGER.warn(
              String.format(
                  "Shortcut %s => %s in %s connector shortcuts overlaps with global shortcuts and will be ignored",
                  shortcut, target, connectorName));
        }
      }
    }
  }
}
