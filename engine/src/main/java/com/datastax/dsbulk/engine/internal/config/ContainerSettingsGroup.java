/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.config;

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
