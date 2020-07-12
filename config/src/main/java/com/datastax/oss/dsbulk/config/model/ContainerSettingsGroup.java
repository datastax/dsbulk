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
package com.datastax.oss.dsbulk.config.model;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A Group that contains settings at a particular level in the hierarchy, and possibly includes all
 * settings below that level.
 */
class ContainerSettingsGroup implements SettingsGroup {

  private final Set<String> settings;
  private final String prefix;

  // Indicates whether this group should include settings that are descendants,
  // as opposed to just immediate children.
  private final boolean includeDescendants;

  ContainerSettingsGroup(String path, boolean includeDescendants, Comparator<String> comparator) {
    prefix = path + ".";
    this.includeDescendants = includeDescendants;
    settings = new TreeSet<>(comparator);
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
