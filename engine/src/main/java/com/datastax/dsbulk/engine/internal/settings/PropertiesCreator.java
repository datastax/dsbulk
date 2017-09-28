/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.SettingsDocumentor;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.io.File;
import java.io.PrintWriter;
import java.util.Map;
import org.apache.commons.text.WordUtils;

public class PropertiesCreator {

  private static final int LINE_LENGTH = 100;

  public static void main(String[] args) {
    try {
      assert args.length == 2;
      String outFile = args[0];
      boolean template = Boolean.parseBoolean(args[1]);
      File file = new File(outFile);
      //noinspection ResultOfMethodCallIgnored
      file.getParentFile().mkdirs();
      PrintWriter pw = new PrintWriter(file);
      LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk"));
      String rowOfHashes = StringUtils.nCopies("#", LINE_LENGTH);
      pw.println(rowOfHashes);
      pw.println(wrapLines(
              "# This is a template configuration file. Uncomment settings as needed to configure "
                  + "DSBulk. When this file is named application.properties and placed in the "
                  + "/conf directory, it will be automatically picked up and used by default. "
                  + "To use other properties files see the -f command-line option."));
      pw.println(rowOfHashes);
      pw.println("");

      for (Map.Entry<String, SettingsDocumentor.Group> groupEntry :
          SettingsDocumentor.GROUPS.entrySet()) {
        String section = groupEntry.getKey();
        if (section.equals("Common")) {
          // In this context, we don't care about the "Common" pseudo-section.
          continue;
        }
        pw.println(rowOfHashes);
        config
            .getConfig(section)
            .root()
            .origin()
            .comments()
            .forEach(
                l -> {
                  pw.print("# ");
                  pw.println(wrapLines(l));
                });
        pw.println(rowOfHashes);

        for (String settingName : groupEntry.getValue().getSettings()) {
          ConfigValue value = config.getValue(settingName);

          pw.println();
          value
              .origin()
              .comments()
              .forEach(
                  l -> {
                    pw.print("# ");
                    pw.println(wrapLines(l));
                  });
          pw.print("# Type: ");
          pw.println(config.getTypeString(settingName));
          pw.print("# Default value: ");
          pw.println(value.render(ConfigRenderOptions.concise()));
          if (template) {
            pw.print("#");
          }
          pw.print(settingName);
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

  private static String wrapLines(String text) {
    return WordUtils.wrap(
        text,
        LINE_LENGTH,
        String.format("%n# "),
        false);
  }
}
