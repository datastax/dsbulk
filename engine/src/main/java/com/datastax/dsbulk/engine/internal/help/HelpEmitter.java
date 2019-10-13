/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.help;

import static com.datastax.dsbulk.engine.internal.utils.ConsoleUtils.LINE_LENGTH;

import com.datastax.dsbulk.commons.internal.config.LoaderConfigFactory;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.config.SettingsGroup;
import com.datastax.dsbulk.engine.internal.config.SettingsGroupFactory;
import com.datastax.dsbulk.engine.internal.config.ShortcutsFactory;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.fusesource.jansi.Ansi;

public class HelpEmitter {

  private static final Pattern CONNECTOR_SETTINGS_PAT =
      Pattern.compile("dsbulk\\.connector\\.[^.]+\\..+");

  private static final int INDENT = 4;

  public static void emitGlobalHelp(@Nullable String connectorName) {

    ConfigFactory.invalidateCaches();
    Config referenceConfig = LoaderConfigFactory.createReferenceConfig();

    Map<String, SettingsGroup> groups = SettingsGroupFactory.createGroups(referenceConfig);

    Map<String, String> longToShortOptions =
        ShortcutsFactory.createShortcutsMap(referenceConfig, connectorName).inverse();

    List<HelpEntry> entries;
    if (connectorName == null) {
      // Include all connector settings, but no shortcuts for connectors
      entries =
          HelpEntryFactory.createEntries(
              groups.get("Common").getSettings(), longToShortOptions, referenceConfig);
    } else {
      // Filter common settings to exclude settings for connectors other than connectorName.
      List<String> commonSettings =
          groups.get("Common").getSettings().stream()
              .filter(
                  name ->
                      name.startsWith("dsbulk.connector." + connectorName + ".")
                          || !CONNECTOR_SETTINGS_PAT.matcher(name).matches())
              .collect(Collectors.toList());

      entries = HelpEntryFactory.createEntries(commonSettings, longToShortOptions, referenceConfig);
    }

    // Add global help options
    entries.add(0, HelpEntryFactory.VERSION_OPTION);
    entries.add(1, HelpEntryFactory.HELP_OPTION);
    entries.add(2, HelpEntryFactory.CONFIG_FILE_OPTION);

    System.out.println(WorkflowUtils.getBulkLoaderNameAndVersion());

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

    renderHelpEntries(entries);

    String footer =
        "GETTING MORE HELP\n\nThere are many more settings/options that may be used to "
            + "customize behavior. Run the `help` command with one of the following section "
            + "names for more details:\n    "
            + String.join("\n    ", getGroupNamesWithoutCommon(groups))
            + "\n\nYou can also find more help at "
            + "https://docs.datastax.com/en/dsbulk/doc/index.html.";

    renderWrappedTextPreformatted(footer);
  }

  public static void emitSectionHelp(@NonNull String sectionName, @Nullable String connectorName) {

    ConfigFactory.invalidateCaches();
    Config referenceConfig = LoaderConfigFactory.createReferenceConfig();

    Map<String, SettingsGroup> groups = SettingsGroupFactory.createGroups(referenceConfig);

    if (!groups.containsKey(sectionName)) {
      // Write error message, available group names, raise as error.
      throw new IllegalArgumentException(
          String.format(
              "%s is not a valid section. Available sections include the following:%n    %s",
              sectionName, String.join("\n    ", getGroupNamesWithoutCommon(groups))));
    }

    // derive connector name from section name if section is a connector
    if (sectionName.startsWith("connector.")) {
      connectorName = sectionName.substring("connector.".length());
    }

    Map<String, String> longToShortOptions =
        ShortcutsFactory.createShortcutsMap(referenceConfig, connectorName).inverse();

    List<HelpEntry> entries =
        HelpEntryFactory.createEntries(
            groups.get(sectionName).getSettings(), longToShortOptions, referenceConfig);

    Set<String> subSections =
        groups.keySet().stream()
            .filter(s -> s.startsWith(sectionName + "."))
            .collect(Collectors.toSet());

    System.out.println(WorkflowUtils.getBulkLoaderNameAndVersion());

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

    renderHelpEntries(entries);

    if (!subSections.isEmpty()) {
      String footer =
          "This section has the following subsections you may be interested in:\n    "
              + String.join("\n    ", subSections);
      renderWrappedTextPreformatted(footer);
    }
  }

  @NonNull
  private static Set<String> getGroupNamesWithoutCommon(Map<String, SettingsGroup> groups) {
    Set<String> groupNames = new LinkedHashSet<>(groups.keySet());
    groupNames.remove("Common");
    return groupNames;
  }

  private static void renderHelpEntries(List<HelpEntry> entries) {
    for (HelpEntry option : entries) {
      String shortOpt = option.getShortOption();
      String longOpt = option.getLongOption();
      String argumentType = option.getArgumentType();
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
      if (argumentType != null) {
        message = message.fgYellow().a(" <").a(argumentType).a(">").reset();
      }
      message = message.newline();
      message = renderWrappedText(message, option.getDescription());
      message = message.newline();
      System.out.print(message);
    }
  }

  private static int findWrapPos(String description) {
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

  private static Ansi renderWrappedText(Ansi message, String text) {
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

      String line = text.substring(0, pos);
      String trimmed = CharMatcher.whitespace().trimTrailingFrom(line);
      message = message.a(trimmed).newline();
    }
  }

  private static void renderWrappedTextPreformatted(String text) {
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
}
