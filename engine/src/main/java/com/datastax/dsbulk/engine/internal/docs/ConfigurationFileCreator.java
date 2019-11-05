/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.docs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.LoaderConfigFactory;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.internal.config.SettingsGroup;
import com.datastax.dsbulk.engine.internal.config.SettingsGroupFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.text.WordUtils;

public class ConfigurationFileCreator {

  private static final int LINE_LENGTH = 100;
  private static final int INDENT_LENGTH = 4;

  private static final String LINE_INDENT = StringUtils.nCopies(" ", INDENT_LENGTH);

  private static final String INDENTED_ROW_OF_HASHES =
      LINE_INDENT + StringUtils.nCopies("#", LINE_LENGTH - INDENT_LENGTH);
  private static final String ROW_OF_HASHES = StringUtils.nCopies("#", LINE_LENGTH);

  private static final Comparator<Entry<String, ConfigValue>> ORIGIN_COMPARATOR =
      (e1, e2) -> {
        String s1 = e1.getValue().origin().description();
        String s2 = e2.getValue().origin().description();
        return s1.compareTo(s2);
      };

  public static void main(String[] args) throws IOException {
    try {
      if (args.length != 1) {
        throw new IllegalArgumentException(
            "Usage: ConfigurationFileCreator \"/path/to/destination/directory\"");
      }
      Path dest = Paths.get(args[0]);
      Files.createDirectories(dest);
      Path dsbulkConfigurationFile = dest.resolve("application.template.conf");
      try (PrintWriter pw =
          new PrintWriter(
              Files.newBufferedWriter(
                  dsbulkConfigurationFile, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
        printDSBulkConfiguration(pw);
      }
      Path driverConfigurationPath = dest.resolve("driver.template.conf");
      try (PrintWriter pw =
          new PrintWriter(
              Files.newBufferedWriter(
                  driverConfigurationPath, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING))) {
        printDriverConfiguration(pw);
      }
    } catch (Exception e) {
      System.err.println("Error encountered generating template configuration file");
      e.printStackTrace();
      throw e;
    }
  }

  private static void printDSBulkConfiguration(PrintWriter pw) {
    pw.println(ROW_OF_HASHES);
    pw.println("# This is a template configuration file for the DataStax Bulk Loader (DSBulk).");
    pw.println("#");
    pw.println("# This file is written in HOCON format; see");
    pw.println("# https://github.com/typesafehub/config/blob/master/HOCON.md");
    pw.println("# for more information on its syntax.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Uncomment settings as needed to configure "
                + "DSBulk. When this file is named application.conf and placed in the "
                + "/conf directory, it will be automatically picked up and used by default. "
                + "To use other file names see the -f command-line option."));
    pw.println(ROW_OF_HASHES);
    pw.println("");
    pw.println("# Java Driver settings");
    pw.println("include url(\"file:./driver.conf\")");
    pw.println("");
    pw.println("dsbulk {");
    pw.println("");

    Config referenceConfig = LoaderConfigFactory.createReferenceConfig();
    Map<String, SettingsGroup> groups =
        SettingsGroupFactory.createDSBulkConfigurationGroups(referenceConfig);

    for (Map.Entry<String, SettingsGroup> groupEntry : groups.entrySet()) {
      String section = groupEntry.getKey();
      if (section.equals("Common")) {
        // In this context, we don't care about the "Common" pseudo-section.
        continue;
      }
      pw.println(INDENTED_ROW_OF_HASHES);
      referenceConfig.getConfig(section).root().origin().comments().stream()
          .filter(line -> !ConfigUtils.isTypeHint(line))
          .filter(line -> !ConfigUtils.isLeaf(line))
          .forEach(
              l -> {
                pw.print(LINE_INDENT + "# ");
                pw.println(wrapIndentedLines(l, 1));
              });
      pw.println(INDENTED_ROW_OF_HASHES);

      for (String settingName : groupEntry.getValue().getSettings()) {
        ConfigValue value = ConfigUtils.getNullSafeValue(referenceConfig, settingName);

        pw.println();
        value.origin().comments().stream()
            .filter(line -> !ConfigUtils.isTypeHint(line))
            .filter(line -> !ConfigUtils.isLeaf(line))
            .forEach(
                l -> {
                  pw.print(LINE_INDENT + "# ");
                  pw.println(wrapIndentedLines(l, 1));
                });
        pw.print(LINE_INDENT + "# Type: ");
        pw.println(ConfigUtils.getTypeString(referenceConfig, settingName).orElse("arg"));
        pw.print(LINE_INDENT + "# Default value: ");
        pw.println(value.render(ConfigRenderOptions.concise()));
        pw.print(LINE_INDENT);
        pw.print("#");
        pw.print(settingName.substring("dsbulk.".length()));
        pw.print(" = ");
        pw.println(value.render(ConfigRenderOptions.concise()));
      }
      pw.println();
    }
    pw.println("}");
  }

  private static void printDriverConfiguration(PrintWriter pw) {
    pw.println(ROW_OF_HASHES);
    pw.println("# Java Driver configuration for DSBulk.");
    pw.println("#");
    pw.println(
        "# The settings below are just a subset of all the configurable options of the driver, and provide");
    pw.println(
        "# an optimal driver configuration for DSBulk for most use cases. See the Java Driver configuration");
    pw.println("# reference for instructions on how to configure the driver properly:");
    pw.println(
        "# https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/configuration");
    pw.println("#");
    pw.println("# This file is written in HOCON format; see");
    pw.println("# https://github.com/typesafehub/config/blob/master/HOCON.md");
    pw.println("# for more information on its syntax.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Uncomment settings as needed to configure the driver. "
                + "When this file is named driver.conf and placed "
                + "in the /conf directory, it will be automatically picked up and used by default."));
    pw.println(ROW_OF_HASHES);
    pw.println("");
    pw.println("datastax-java-driver {");
    pw.println("");
    Config dsbulkStandaloneConfig = LoaderConfigFactory.standaloneDSBulkReference();
    Config driverConfig = dsbulkStandaloneConfig.getConfig("datastax-java-driver");
    Set<Entry<String, ConfigValue>> entries = new TreeSet<>(ORIGIN_COMPARATOR);
    entries.addAll(driverConfig.root().entrySet());
    for (Entry<String, ConfigValue> groupEntry : entries) {
      printDriverSetting(pw, driverConfig, groupEntry, 1);
    }
    pw.println("}");
  }

  private static void printDriverSetting(
      PrintWriter pw, Config driverConfig, Entry<String, ConfigValue> groupEntry, int indentation) {
    String settingName = groupEntry.getKey();
    ConfigValue value = ConfigUtils.getNullSafeValue(driverConfig, settingName);
    String spaces = StringUtils.nCopies(" ", INDENT_LENGTH * indentation);
    if (ConfigUtils.isLeaf(value)) {
      value.origin().comments().stream()
          .filter(line -> !ConfigUtils.isTypeHint(line))
          .filter(line -> !ConfigUtils.isLeaf(line))
          .forEach(
              l -> {
                pw.print(spaces + "# ");
                pw.println(wrapIndentedLines(l, indentation));
              });
      pw.print(spaces + "# Type: ");
      pw.println(ConfigUtils.getTypeString(driverConfig, settingName).orElse("arg"));
      pw.print(spaces + "# Default value: ");
      pw.println(value.render(ConfigRenderOptions.concise()));
      pw.print(spaces);
      pw.print("#");
      pw.print(settingName);
      pw.print(" = ");
      pw.println(value.render(ConfigRenderOptions.concise()));
      pw.println();
    } else {
      value.origin().comments().stream()
          .filter(line -> !ConfigUtils.isTypeHint(line))
          .filter(line -> !ConfigUtils.isLeaf(line))
          .forEach(
              l -> {
                pw.print(spaces + "# ");
                pw.println(wrapIndentedLines(l, indentation));
              });
      pw.print(spaces);
      pw.print(settingName);
      pw.println(" {");
      pw.println();
      Set<Entry<String, ConfigValue>> entries = new TreeSet<>(ORIGIN_COMPARATOR);
      entries.addAll(((ConfigObject) value).entrySet());
      for (Entry<String, ConfigValue> e : entries) {
        printDriverSetting(pw, ((ConfigObject) value).toConfig(), e, indentation + 1);
      }
      pw.print(spaces);
      pw.println("}");
      pw.println();
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static String wrapLines(String text) {
    return WordUtils.wrap(text, LINE_LENGTH - 2, String.format("%n# "), false);
  }

  private static String wrapIndentedLines(String text, int indentation) {
    return WordUtils.wrap(
        text,
        (LINE_LENGTH - INDENT_LENGTH * indentation) - 2,
        String.format("%n%s# ", StringUtils.nCopies(" ", INDENT_LENGTH * indentation)),
        false);
  }
}
