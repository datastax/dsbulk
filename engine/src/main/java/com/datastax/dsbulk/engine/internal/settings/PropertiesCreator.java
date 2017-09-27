/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.File;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.text.WordUtils;

public class PropertiesCreator {
  public static void main(String[] args) {
    try {
      assert args.length == 2;
      String directory = args[0];
      boolean template = Boolean.parseBoolean(args[1]);
      File file = new File(directory);
      PrintWriter pw = new PrintWriter(file);
      Config config = ConfigFactory.load().getConfig("dsbulk");
      TreeSet<String> sections = new TreeSet<>(config.root().keySet());
      for (String section : sections) {

        pw.println(
            "###################################################################################################");
        config
            .getConfig(section)
            .root()
            .origin()
            .comments()
            .forEach(
                l -> {
                  pw.print("# ");
                  pw.println(WordUtils.wrap(l, 100, String.format("%n# "), false));
                });
        pw.println(
            "###################################################################################################");

        Set<Map.Entry<String, ConfigValue>> entries = config.withOnlyPath(section).entrySet();
        TreeMap<String, ConfigValue> sorted = new TreeMap<>();
        for (Map.Entry<String, ConfigValue> entry : entries) {
          sorted.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, ConfigValue> entry : sorted.entrySet()) {
          String key = entry.getKey();
          ConfigValue value = entry.getValue();

          pw.println();
          value
              .origin()
              .comments()
              .forEach(
                  l -> {
                    pw.print("# ");
                    pw.println(WordUtils.wrap(l, 100, String.format("%n# "), false));
                  });
          pw.print("# Type: ");
          pw.println(getType(value));
          pw.print("# Default value: ");
          pw.println(value.render(ConfigRenderOptions.concise()));
          if (template) {
            pw.print("#");
          }
          pw.print(key);
          pw.print(" = ");
          pw.println(value.render(ConfigRenderOptions.concise()));
        }
        pw.println();
        pw.println();
      }
      pw.flush();
    } catch (Exception e) {
      System.out.println("Error encountered generating reference.conf");
      e.printStackTrace();
    }
  }

  private static String getType(ConfigValue value) {
    ConfigValueType type = value.valueType();
    if (type == ConfigValueType.LIST) {
      ConfigList list = ((ConfigList) value);
      if (!list.isEmpty()) {
        return getType(list.get(0)) + " LIST";
      }
    }
    return type.toString();
  }
}
