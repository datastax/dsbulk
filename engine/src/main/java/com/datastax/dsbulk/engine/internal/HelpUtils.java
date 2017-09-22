/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.settings.StringUtils;
import com.google.common.base.CharMatcher;
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
import java.util.TreeSet;
import java.util.stream.Collectors;
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
    PrintWriter pw = new PrintWriter(System.out);
    HelpEmitter helpEmitter = new HelpEmitter(options);
    helpEmitter.emit(pw, footer);
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

  private static class HelpEmitter {

    private static final String HEADER =
        "Usage: dsbulk (load|unload) [options]\n       dsbulk help [section]\nOptions:";

    private static final int LINE_LENGTH = getLineLength();
    private static final int INDENT = 4;

    private final Set<Option> options =
        new TreeSet<>(new PriorityComparator(SettingsDocumentor.PREFERRED_SETTINGS));

    private static int getLineLength() {
      String columns = System.getenv("COLUMNS");
      if (columns != null) {
        return Integer.parseInt(columns);
      }
      return 150;
    }

    HelpEmitter(Options options) {
      this.options.addAll(options.getOptions());
    }

    void emit(PrintWriter writer, String footer) {
      writer.println(getVersionMessage());
      writer.println(HEADER);
      for (Option option : options) {
        String shortOpt = option.getOpt();
        String longOpt = option.getLongOpt();
        if (shortOpt != null) {
          writer.print("-");
          writer.print(shortOpt);
          if (longOpt != null) {
            writer.print(", ");
          }
        }
        if (longOpt != null) {
          writer.print("--");
          writer.print(longOpt);
        }
        writer.print(" <");
        writer.print(option.getArgName());
        writer.println(">");
        renderWrappedText(writer, option.getDescription());
        writer.println("");
      }
      if (footer != null) {
        writer.println(footer);
      }
    }

    private int findWrapPos(String description, int lineLength) {
      // NB: Adapted from commons-cli HelpFormatter.findWrapPos

      // The line ends before the max wrap pos or a new line char found
      int pos = description.indexOf('\n');
      if (pos != -1 && pos <= lineLength) {
        return pos + 1;
      }

      // The remainder of the description (starting at startPos) fits on
      // one line.
      if (lineLength >= description.length()) {
        return -1;
      }

      // Look for the last whitespace character before startPos + LINE_LENGTH
      for (pos = lineLength; pos >= 0; --pos) {
        final char c = description.charAt(pos);
        if (c == ' ' || c == '\n' || c == '\r') {
          break;
        }
      }

      // If we found it - just return
      if (pos > 0) {
        return pos;
      }

      // If we didn't find one, simply chop at LINE_LENGTH
      return lineLength;
    }

    private void renderWrappedText(PrintWriter writer, String text) {
      // NB: Adapted from commons-cli HelpFormatter.renderWrappedText
      int indent = INDENT;
      if (indent >= LINE_LENGTH) {
        // stops infinite loop happening
        indent = 1;
      }

      // all lines must be padded with indent space characters
      final String padding = StringUtils.nCopies(" ", indent);
      text = padding + text.trim();

      int pos = 0;

      while (true) {
        text = padding + text.substring(pos).trim();
        pos = findWrapPos(text, LINE_LENGTH);

        if (pos == -1) {
          writer.println(text);
          return;
        }

        if (text.length() > LINE_LENGTH && pos == indent - 1) {
          pos = LINE_LENGTH;
        }

        writer.println(CharMatcher.whitespace().trimTrailingFrom(text.substring(0, pos)));
      }
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
}
