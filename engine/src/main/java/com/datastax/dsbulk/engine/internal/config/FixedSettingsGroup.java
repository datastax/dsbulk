/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.config;

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
