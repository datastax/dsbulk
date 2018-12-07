/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.COMMON_SETTINGS;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.CONFIG_FILE_OPTION;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.GROUPS;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.PREFERRED_SETTINGS;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.WorkflowType;
import com.google.common.base.CharMatcher;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.jetbrains.annotations.NotNull;

public class HelpUtils {

  private static final Pattern CONNECTOR_SETTINGS_PAT = Pattern.compile("connector\\.[^.]+\\..+");
  private static final String COLUMNS_ENV_NAME = "COLUMNS";
  private static final int DEFAULT_LINE_LENGTH = 150;
  private static final int LINE_LENGTH = getLineLength();

  public static void emitSectionHelp(String sectionName) {
    if (!GROUPS.containsKey(sectionName)) {
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
            GROUPS.get(sectionName).getSettings(),
            SettingsUtils.getLongToShortOptionsMap(connectorName));
    Set<String> subSections =
        GROUPS
            .keySet()
            .stream()
            .filter(s -> s.startsWith(sectionName + "."))
            .collect(Collectors.toSet());
    String footer = null;
    if (!subSections.isEmpty()) {
      footer =
          "This section has the following subsections you may be interested in:\n    "
              + String.join("\n    ", subSections);
    }
    emitHelp(options, footer, sectionName);
  }

  public static void emitGlobalHelp(String connectorName) {
    List<String> commonSettings = COMMON_SETTINGS;
    if (connectorName != null) {
      // Filter common settings to exclude settings for connectors other than connectorName.
      String settingPrefix = "connector." + connectorName + ".";
      commonSettings =
          commonSettings
              .stream()
              .filter(
                  name ->
                      name.startsWith(settingPrefix)
                          || !CONNECTOR_SETTINGS_PAT.matcher(name).matches())
              .collect(Collectors.toList());
    }
    Options options =
        createOptions(commonSettings, SettingsUtils.getLongToShortOptionsMap(connectorName));
    options.addOption(CONFIG_FILE_OPTION);
    String footer =
        "GETTING MORE HELP\n\nThere are many more settings/options that may be used to "
            + "customize behavior. Run the `help` command with one of the following section "
            + "names for more details:\n    "
            + String.join("\n    ", getGroupNames())
            + "\n\nYou can also find more help at "
            + "https://docs.datastax.com/en/dsbulk/doc/index.html.";
    emitHelp(options, footer, null);
  }

  private static void emitHelp(Options options, String footer, String sectionName) {
    AnsiConsole.systemInstall();
    HelpEmitter helpEmitter = new HelpEmitter(options, sectionName);
    helpEmitter.emit(footer);
  }

  public static String getVersionMessage() {
    // Get the version of dsbulk from version.txt.
    String version = "UNKNOWN";
    try (InputStream versionStream = DataStaxBulkLoader.class.getResourceAsStream("/version.txt")) {
      if (versionStream != null) {
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(versionStream, StandardCharsets.UTF_8));
        version = reader.readLine();
      }
    } catch (Exception e) {
      // swallow
    }
    return String.format("DataStax Bulk Loader v%s", version);
  }

  public static int getLineLength() {
    int columns = DEFAULT_LINE_LENGTH;
    String columnsStr = System.getenv(COLUMNS_ENV_NAME);
    if (columnsStr != null) {
      try {
        columns = Integer.parseInt(columnsStr);
      } catch (NumberFormatException ignored) {
      }
      if (PlatformUtils.isWindows()) {
        columns--;
      }
    }
    return columns;
  }

  @NotNull
  private static Set<String> getGroupNames() {
    Set<String> groupNames = GROUPS.keySet();
    groupNames.remove("Common");
    return groupNames;
  }

  private static Options createOptions(
      Collection<String> settings, Map<String, String> longToShortOptions) {
    Options options = new Options();
    for (String setting : settings) {
      options.addOption(
          OptionUtils.createOption(
              setting,
              ConfigUtils.getNullSafeValue(DataStaxBulkLoader.DEFAULT, setting),
              longToShortOptions));
    }
    return options;
  }

  private static class HelpEmitter {

    private static final int INDENT = 4;

    private final Set<Option> options = new TreeSet<>(new PriorityComparator(PREFERRED_SETTINGS));
    private final String sectionName;

    HelpEmitter(Options options, String sectionName) {
      this.sectionName = sectionName;
      this.options.addAll(options.getOptions());
    }

    void emit(String footer) {
      System.out.println(getVersionMessage());

      if (sectionName == null) {
        Ansi header =
            Ansi.ansi()
                .a("Usage: ")
                .fgRed()
                .a("dsbulk <command> [options]")
                .newline()
                .a("       dsbulk help [section]")
                .reset()
                .newline();
        System.out.println(header);

        Ansi commands = Ansi.ansi().a("Available commands:").reset().newline().newline();
        for (WorkflowType workflowType : WorkflowType.values()) {
          commands = commands.fgCyan().a(workflowType.getTitle()).reset().a(":").newline();
          commands = renderWrappedText(commands, workflowType.getDescription());
          commands = commands.newline();
        }
        System.out.println(commands);

        Ansi options = Ansi.ansi().a("Common options:").reset().newline();
        System.out.println(options);

      } else {
        Ansi header =
            Ansi.ansi()
                .a("Help for section: ")
                .fgRed()
                .a(sectionName)
                .reset()
                .a(" (run `dsbulk help` to get the global help).")
                .newline();
        System.out.println(header);
        Ansi options = Ansi.ansi().a("Options in this section:").reset().newline();
        System.out.println(options);
      }

      for (Option option : this.options) {
        String shortOpt = option.getOpt();
        String longOpt = option.getLongOpt();
        Ansi message = Ansi.ansi();
        if (shortOpt != null) {
          message = message.fgCyan().a("-").a(shortOpt);
          if (longOpt != null) {
            message = message.reset().a(", ");
          }
        }
        if (longOpt != null) {
          message = message.fgGreen().a("--").a(longOpt).reset();
        }
        message = message.fgYellow().a(" <").a(option.getArgName()).a(">").reset().newline();
        message = renderWrappedText(message, option.getDescription());
        message = message.newline();
        System.out.print(message.toString());
      }
      if (footer != null) {
        renderWrappedTextPreformatted(footer);
      }
    }

    private int findWrapPos(String description) {
      // NB: Adapted from commons-cli HelpFormatter.findWrapPos

      // The line ends before the max wrap pos or a new line char found
      int pos = description.indexOf('\n');
      if (pos != -1 && pos <= LINE_LENGTH) {
        return pos + 1;
      }

      // The remainder of the description (starting at startPos) fits on
      // one line.
      if (LINE_LENGTH >= description.length()) {
        return -1;
      }

      // Look for the last whitespace character before startPos + LINE_LENGTH
      for (pos = LINE_LENGTH; pos >= 0; --pos) {
        char c = description.charAt(pos);
        if (c == ' ' || c == '\n' || c == '\r') {
          break;
        }
      }

      // If we found it - just return
      if (pos > 0) {
        return pos;
      }

      // If we didn't find one, simply chop at LINE_LENGTH
      return LINE_LENGTH;
    }

    private Ansi renderWrappedText(Ansi message, String text) {
      // NB: Adapted from commons-cli HelpFormatter.renderWrappedText
      int indent = INDENT;
      if (indent >= LINE_LENGTH) {
        // stops infinite loop happening
        indent = 1;
      }

      // all lines must be padded with indent space characters
      String padding = StringUtils.nCopies(" ", indent);
      text = padding + text.trim();

      int pos = 0;

      while (true) {
        text = padding + text.substring(pos).trim();
        pos = findWrapPos(text);

        if (pos == -1) {
          return message.a(text).newline();
        }

        if (text.length() > LINE_LENGTH && pos == indent - 1) {
          pos = LINE_LENGTH;
        }

        message =
            message.a(CharMatcher.whitespace().trimTrailingFrom(text.substring(0, pos))).newline();
      }
    }

    private void renderWrappedTextPreformatted(String text) {
      // NB: Adapted from commons-cli HelpFormatter.renderWrappedText
      int pos = 0;

      while (true) {
        text = text.substring(pos);
        if (text.charAt(0) == ' ' && text.charAt(1) != ' ') {
          // The last line is long, and the wrap-around occurred at the end of a word,
          // and we have a space as our first character in the new line. Remove it.
          // This doesn't universally trim spaces because pre-formatted text may have
          // leading spaces intentionally. We assume there are more than one of space
          // in those cases, and don't trim then.

          text = text.trim();
        }
        pos = findWrapPos(text);

        if (pos == -1) {
          System.out.println(text);
          return;
        }

        System.out.println(CharMatcher.whitespace().trimTrailingFrom(text.substring(0, pos)));
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
