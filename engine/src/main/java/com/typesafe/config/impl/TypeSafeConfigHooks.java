/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.typesafe.config.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TypeSafeConfigHooks {

  public static Set<Map.Entry<String, ConfigValue>> entrySetWithNulls(Config config) {
    Set<Map.Entry<String, ConfigValue>> entries = new HashSet<Map.Entry<String, ConfigValue>>();
    findPaths(entries, null, (AbstractConfigObject) config.root());
    return entries;
  }

  private static void findPaths(
      Set<Map.Entry<String, ConfigValue>> entries, Path parent, AbstractConfigObject obj) {
    for (Map.Entry<String, ConfigValue> entry : obj.entrySet()) {
      String elem = entry.getKey();
      ConfigValue v = entry.getValue();
      Path path = Path.newKey(elem);
      if (parent != null) {
        path = path.prepend(parent);
      }
      if (v instanceof AbstractConfigObject) {
        findPaths(entries, path, (AbstractConfigObject) v);
      } else {
        entries.add(new AbstractMap.SimpleImmutableEntry<>(path.render(), v));
      }
    }
  }
}
