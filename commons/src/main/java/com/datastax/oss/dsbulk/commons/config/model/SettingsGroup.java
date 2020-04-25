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

import java.util.Set;

/**
 * Encapsulates a group of settings that should be rendered together. When adding a setting to a
 * group, the add may be rejected because the given setting doesn't fit the criteria of settings
 * that belong to the group. This allows groups to act as listeners for settings and accept only
 * those that they are interested in.
 */
public interface SettingsGroup {

  boolean addSetting(String settingName);

  Set<String> getSettings();
}
