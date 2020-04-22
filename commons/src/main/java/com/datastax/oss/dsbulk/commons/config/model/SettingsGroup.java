/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
