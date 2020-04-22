/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
