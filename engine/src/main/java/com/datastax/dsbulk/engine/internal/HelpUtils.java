/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.Main;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

public class HelpUtils {
  public static void emitSectionHelp(String sectionName) {
    if (!SettingsDocumentor.GROUPS.containsKey(sectionName)) {
      // Write error message, available group names, raise as error.
      throw new IllegalArgumentException(
          String.format(
              "%s is not a valid section. Available sections include the following:%n    %s",
              sectionName, String.join("\n    ", getGroupNames())));
    }

    String connectorName = null;
    if (sectionName.startsWith("connector.")) {
      connectorName = sectionName.substring("connector.".length());
    }
    Options options =
        createOptions(
            SettingsDocumentor.GROUPS.get(sectionName).getSettings(),
            OptionUtils.getLongToShortMap(connectorName));
    Set<String> subSections =
        SettingsDocumentor.GROUPS
            .keySet()
            .stream()
            .filter(s -> s.startsWith(sectionName + "."))
            .collect(Collectors.toSet());
    String footer = null;
    if (!subSections.isEmpty()) {
      footer =
          "\nThis section has the following subsections you may be interested in:\n    "
              + String.join("\n    ", subSections);
    }
    emitHelp(options, footer);
  }

  public static void emitGlobalHelp() {
    Options options =
        createOptions(SettingsDocumentor.COMMON_SETTINGS, OptionUtils.getLongToShortMap(null));
    options.addOption(SettingsDocumentor.CONFIG_FILE_OPTION);
    String footer =
        "GETTING MORE HELP\n\nThere are many more settings/options that may be used to "
            + "customize behavior. Run the `help` command with one of the following section "
            + "names for more details:\n    "
            + String.join("\n    ", getGroupNames());
    emitHelp(options, footer);
  }

  private static void emitHelp(Options options, String footer) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new PriorityComparator(SettingsDocumentor.PREFERRED_SETTINGS));
    PrintWriter pw = new PrintWriter(System.out);
    pw.println(getVersionMessage());
    formatter.printHelp(
        pw,
        150,
        "dsbulk (load|unload) [options]\n       dsbulk help [section]\n",
        "options:",
        options,
        0,
        5,
        footer);
    pw.flush();
  }

  public static String getVersionMessage() {
    // Get the version of dsbulk from version.txt.
    String version = "UNKNOWN";
    try (InputStream versionStream = Main.class.getResourceAsStream("/version.txt")) {
      if (versionStream != null) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(versionStream));
        version = reader.readLine();
      }
    } catch (Exception e) {
      // swallow
    }
    return String.format("DataStax Bulk Loader v%s", version);
  }

  @NotNull
  private static Set<String> getGroupNames() {
    Set<String> groupNames = SettingsDocumentor.GROUPS.keySet();
    groupNames.remove("Common");
    return groupNames;
  }

  private static Options createOptions(
      Collection<String> settings, Map<String, String> longToShortOptions) {
    Options options = new Options();

    LoaderConfig config = new DefaultLoaderConfig(OptionUtils.DEFAULT);
    for (String setting : settings) {
      options.addOption(
          OptionUtils.createOption(config, longToShortOptions, setting, config.getValue(setting)));
    }
    return options;
  }

  /**
   * Options comparator that supports placing "high priority" values first. This allows a setting
   * group to have "mostly" alpha-sorted settings, but with certain settings promoted to be first
   * (and thus emitted first when generating documentation).
   */
  private static class PriorityComparator implements Comparator<Option> {
    private final Map<String, Integer> prioritizedValues;

    PriorityComparator(List<String> highPriorityValues) {
      prioritizedValues = new HashMap<>();
      int counter = 0;
      for (String s : highPriorityValues) {
        prioritizedValues.put(s, counter++);
      }
    }

    @Override
    public int compare(Option left, Option right) {
      // Ok, this is kinda hacky, but special case -f, which should be first.
      Integer leftInd =
          "f".equals(left.getOpt())
              ? -1
              : this.prioritizedValues.getOrDefault(left.getLongOpt(), 99999);
      Integer rightInd =
          "f".equals(right.getOpt())
              ? -1
              : this.prioritizedValues.getOrDefault(right.getLongOpt(), 99999);
      int indCompare = leftInd.compareTo(rightInd);

      if (indCompare != 0) {
        return indCompare;
      }

      // Ok, so neither is a prioritized option. Compare the long name.
      return left.getLongOpt().compareTo(right.getLongOpt());
    }
  }
}
