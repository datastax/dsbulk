/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.docs;

import static com.datastax.dsbulk.engine.internal.config.SettingsGroupFactory.ORIGIN_COMPARATOR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.internal.config.SettingsGroup;
import com.datastax.dsbulk.engine.internal.config.SettingsGroupFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
            "# Please make sure you've read the DataStax Bulk Loader documentation "
                + "included in this binary distribution:"));
    pw.println("# ../manual/README.md");
    pw.println("#");
    pw.println("# An exhaustive list of available settings can be found here:");
    pw.println("# ../manual/settings.md");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Also, two template configuration files meant to be used together can be "
                + "found here:"));
    pw.println("# ../manual/application.template.conf");
    pw.println("# ../manual/driver.template.conf");
    pw.println("#");
    pw.println(
        wrapLines(
            "# We recommend that this file be named application.conf and placed in the "
                + "/conf directory; these are indeed the default file name and path where "
                + "DSBulk looks for configuration files."));
    pw.println("#");
    pw.println(
        wrapLines(
            "# To use other file names, or another folder, you can use the -f command "
                + "line switch; consult the DataStax Bulk Loader online documentation for more "
                + "information:"));
    pw.println("# https://docs.datastax.com/en/dsbulk/doc/dsbulk/dsbulkLoadConfigFile.html");
    pw.println(ROW_OF_HASHES);
    pw.println();
    pw.println(ROW_OF_HASHES);
    pw.println("# DataStax Java Driver settings.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# You can declare any Java Driver settings directly in this file, but for "
                + "maintainability sake, we placed them in a separate file, which is "
                + "expected to be named driver.conf and located in the same /conf directory."));
    pw.println(
        wrapLines(
            "# Use that file, for example, to define contact points, provide "
                + "authentication and encryption settings, modify timeouts, consistency levels, "
                + "page sizes, policies, and much more."));
    pw.println(
        wrapLines(
            "# If you decide to declare the driver settings in a different way, or in a "
                + "file named differently, make sure to test your setup to ensure that all "
                + "settings are correctly detected."));
    pw.println("#");
    pw.println("# You can also consult the Java Driver online documentation for more details:");
    pw.println("# https://docs.datastax.com/en/developer/java-driver/latest/");
    pw.println("# https://docs.datastax.com/en/developer/java-driver-dse/latest/");
    pw.println("include classpath(\"driver.conf\")");
    pw.println(ROW_OF_HASHES);
    pw.println();
    pw.println(ROW_OF_HASHES);
    pw.println("# DataStax Bulk Loader settings.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# Settings for the DataStax Bulk Loader (DSBulk) are declared below. Use this "
                + "section, for example, to define which connector to use and how, to customize "
                + "logging, monitoring, codecs, to specify schema settings and mappings, "
                + "and much more."));
    pw.println("#");
    pw.println(
        "# You can also consult the DataStax Bulk Loader online documentation for more details:");
    pw.println("# https://docs.datastax.com/en/dsbulk/doc/dsbulk/dsbulkRef.html");
    pw.println(ROW_OF_HASHES);
    pw.println("dsbulk {");
    pw.println();

    Config referenceConfig = ConfigUtils.createReferenceConfig();

    Map<String, SettingsGroup> groups = SettingsGroupFactory.createDSBulkConfigurationGroups(false);

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
        wrapLines(
            "# The settings below are just a subset of all the configurable options of the "
                + "driver, and provide an optimal driver configuration for DSBulk for most use cases. "
                + "See the Java Driver configuration reference for instructions on how to configure "
                + "the driver properly:"));
    pw.println("# https://docs.datastax.com/en/developer/java-driver/latest/");
    pw.println("# https://docs.datastax.com/en/developer/java-driver-dse/latest/");
    pw.println("#");
    pw.println("# This file is written in HOCON format; see");
    pw.println("# https://github.com/typesafehub/config/blob/master/HOCON.md");
    pw.println("# for more information on its syntax.");
    pw.println("#");
    pw.println(
        wrapLines(
            "# This file is not meant as the main configuration file for DSBulk, but "
                + "rather to be included from the main configuration file. We recommend that "
                + "this file be named driver.conf and placed in the /conf directory, alongside "
                + "with another configuration file for DSBulk itself, named application.conf. "
                + "Also, for this setup to work, application.conf should include driver.conf, "
                + "for example by using an import directive. For other ways to "
                + "configure this tool, refer to DataStax Bulk Loader online documentation:"));
    pw.println("# https://docs.datastax.com/en/dsbulk/doc/dsbulk/dsbulkRef.html");
    pw.println(ROW_OF_HASHES);
    pw.println("");
    pw.println("datastax-java-driver {");
    pw.println("");
    Config driverConfig = ConfigUtils.standaloneDriverReference().getConfig("datastax-java-driver");
    printDriverSettings(pw, driverConfig.root(), 1);
    pw.println("}");
  }

  private static void printDriverSettings(
      @NonNull PrintWriter pw, @NonNull ConfigObject root, int indentation) {
    Set<Entry<String, ConfigValue>> entries = new TreeSet<>(ORIGIN_COMPARATOR);
    entries.addAll(root.entrySet());
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      ConfigValue value = ConfigUtils.getNullSafeValue(root.toConfig(), key);
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
        pw.println(ConfigUtils.getTypeString(root.toConfig(), key).orElse("arg"));
        pw.print(spaces + "# Default value: ");
        pw.println(value.render(ConfigRenderOptions.concise()));
        pw.print(spaces);
        pw.print("#");
        pw.print(key);
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
        pw.print(key);
        pw.println(" {");
        pw.println();
        printDriverSettings(pw, ((ConfigObject) value), indentation + 1);
        pw.print(spaces);
        pw.println("}");
        pw.println();
      }
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
