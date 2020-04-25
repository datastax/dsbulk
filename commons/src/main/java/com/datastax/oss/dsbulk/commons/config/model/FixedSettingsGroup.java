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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** A group of particular settings. */
class FixedSettingsGroup implements SettingsGroup {

  private final Set<String> settings;

  FixedSettingsGroup(List<String> fixedSettings) {
    settings = new TreeSet<>(new SettingsComparator(fixedSettings));
    settings.addAll(fixedSettings);
  }

  @Override
  public boolean addSetting(String settingName) {
    // Always return false because we want other groups to have a chance to add this
    // setting as well.
    return false;
  }

  @Override
  public Set<String> getSettings() {
    return settings;
  }
}
