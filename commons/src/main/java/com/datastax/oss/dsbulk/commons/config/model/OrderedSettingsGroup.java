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

import java.util.LinkedHashSet;
import java.util.Set;

/** A Group that contains settings ordered in the order they were added. */
class OrderedSettingsGroup implements SettingsGroup {

  private final Set<String> settings = new LinkedHashSet<>();

  @Override
  public boolean addSetting(String settingName) {
    return settings.add(settingName);
  }

  @Override
  public Set<String> getSettings() {
    return settings;
  }
}
