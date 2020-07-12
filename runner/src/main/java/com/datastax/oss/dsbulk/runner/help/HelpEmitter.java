/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.runner.help;

import static com.datastax.oss.dsbulk.commons.ConsoleUtils.LINE_LENGTH;

import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.datastax.oss.dsbulk.commons.ConsoleUtils;
import com.datastax.oss.dsbulk.commons.StringUtils;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.config.model.SettingsGroup;
import com.datastax.oss.dsbulk.config.model.SettingsGroupFactory;
import com.datastax.oss.dsbulk.config.shortcuts.ShortcutsFactory;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.fusesource.jansi.Ansi;

public class HelpEmitter {

  private static final Pattern CONNECTOR_SETTINGS_PATTERN =
      Pattern.compile("dsbulk\\.connector\\.[^.]+\\..+");

  private static final int INDENT = 4;

  public static void emitGlobalHelp(@Nullable String connectorName) {

    Map<String, SettingsGroup> groups = SettingsGroupFactory.createDSBulkConfigurationGroups(false);

    Config referenceConfig =
        ConfigUtils.standaloneDSBulkReference()
            .withFallback(ConfigUtils.standaloneDriverReference());

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
                          || !CONNECTOR_SETTINGS_PATTERN.matcher(name).matches())
              .collect(Collectors.toList());

      entries = HelpEntryFactory.createEntries(commonSettings, longToShortOptions, referenceConfig);
    }

    // Add global help options
    entries.add(0, HelpEntryFactory.VERSION_OPTION);
    entries.add(1, HelpEntryFactory.HELP_OPTION);
    entries.add(2, HelpEntryFactory.CONFIG_FILE_OPTION);

    System.out.println(ConsoleUtils.getBulkLoaderNameAndVersion());

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

    renderAvailableCommands();

    Ansi options = Ansi.ansi().a("Common options:").reset().newline();
    System.out.println(options);

    renderAbbreviatedDsbulkOptionNote();
    renderAbbreviatedDriverOptionNote();

    renderHelpEntries(entries);

    renderGettingMoreHelpFooter(groups);
  }

  public static void emitSectionHelp(@NonNull String sectionName, @Nullable String connectorName) {

    boolean driverSection = sectionName.equals("datastax-java-driver");

    Map<String, SettingsGroup> groups =
        SettingsGroupFactory.createDSBulkConfigurationGroups(driverSection);

    if (!groups.containsKey(sectionName)) {
      // Write error message, available group names, raise as error.
      throw new IllegalArgumentException(
          String.format(
              "%s is not a valid section. Available sections include the following:%n    %s",
              sectionName, String.join("\n    ", getSectionNames(groups))));
    }

    // derive connector name from section name if section is a connector
    if (sectionName.startsWith("dsbulk.connector.")) {
      connectorName = sectionName.substring("dsbulk.connector.".length());
    }

    Config referenceConfig =
        ConfigUtils.standaloneDSBulkReference()
            .withFallback(ConfigUtils.standaloneDriverReference());

    Map<String, String> longToShortOptions =
        ShortcutsFactory.createShortcutsMap(referenceConfig, connectorName).inverse();

    List<HelpEntry> entries =
        HelpEntryFactory.createEntries(
            groups.get(sectionName).getSettings(), longToShortOptions, referenceConfig);

    System.out.println(ConsoleUtils.getBulkLoaderNameAndVersion());

    Ansi header =
        Ansi.ansi()
            .a("Help for section: ")
            .fgRed()
            .a(sectionName)
            .reset()
            .a(" (run `dsbulk help` to get the global help).")
            .newline();
    System.out.println(header);

    if (driverSection) {
      renderDriverSpecialInstructions();
    }

    Ansi options = Ansi.ansi().a("Options in this section:").reset().newline();
    System.out.println(options);

    if (driverSection) {
      renderAbbreviatedDriverOptionNote();
    } else {
      renderAbbreviatedDsbulkOptionNote();
    }

    renderHelpEntries(entries);

    renderAvailableSubSections(sectionName, groups);

    if (driverSection) {
      renderDriverFooter();
    }
  }

  private static void renderAvailableCommands() {
    Ansi commands = Ansi.ansi().a("Available commands:").reset().newline().newline();
    ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class);
    for (WorkflowProvider workflowProvider : loader) {
      commands = commands.fgCyan().a(workflowProvider.getTitle()).reset().a(":").newline();
      commands = renderWrappedText(commands, workflowProvider.getDescription());
      commands = commands.newline();
    }
    System.out.println(commands);
  }

  private static void renderAbbreviatedDsbulkOptionNote() {
    Ansi note =
        renderWrappedText(
            Ansi.ansi(),
            "Note: on the command line, long options referring to DSBulk configuration "
                + "settings can have their prefix 'dsbulk' omitted.");
    System.out.println(note);
  }

  private static void renderAbbreviatedDriverOptionNote() {
    Ansi note =
        renderWrappedText(
            Ansi.ansi(),
            "Note: on the command line, long options referring to driver configuration "
                + "settings can be introduced by the prefix 'datastax-java-driver' or just 'driver'.");
    System.out.println(note);
  }

  private static void renderDriverSpecialInstructions() {
    renderWrappedTextPreformatted(
        "Any valid driver setting can be specified on the command line. The options listed below "
            + "are just a subset of all the configurable options of the driver.");
    System.out.println();
  }

  private static void renderDriverFooter() {
    renderWrappedTextPreformatted("See the Java Driver online documentation for more information:");
    renderWrappedTextPreformatted("https://docs.datastax.com/en/developer/java-driver/latest/");
    renderWrappedTextPreformatted("https://docs.datastax.com/en/developer/java-driver-dse/latest/");
  }

  private static void renderAvailableSubSections(
      @NonNull String sectionName, Map<String, SettingsGroup> groups) {
    Set<String> subSections = new HashSet<>();
    for (String s : groups.keySet()) {
      if (s.startsWith(sectionName + ".")) {
        subSections.add(s);
      }
    }
    if (!subSections.isEmpty()) {
      String footer =
          "This section has the following subsections you may be interested in:\n    "
              + String.join("\n    ", subSections);
      renderWrappedTextPreformatted(footer);
    }
  }

  private static void renderGettingMoreHelpFooter(Map<String, SettingsGroup> groups) {
    String footer =
        "GETTING MORE HELP\n\nThere are many more settings/options that may be used to "
            + "customize behavior. Run the `help` command with one of the following section "
            + "names for more details:\n    "
            + String.join("\n    ", getSectionNames(groups))
            + "\n\nYou can also find more help at "
            + "https://docs.datastax.com/en/dsbulk/doc.";

    renderWrappedTextPreformatted(footer);
  }

  @NonNull
  private static Set<String> getSectionNames(Map<String, SettingsGroup> groups) {
    Set<String> groupNames = new LinkedHashSet<>(groups.keySet());
    groupNames.remove("Common");
    groupNames.add("driver");
    return groupNames;
  }

  private static void renderHelpEntries(List<HelpEntry> entries) {
    for (HelpEntry option : entries) {
      String shortOpt = option.getShortOption();
      String abbreviatedLongOpt = option.getAbbreviatedOption();
      String longOpt = option.getLongOption();
      String argumentType = option.getArgumentType();
      Ansi message = Ansi.ansi();
      if (shortOpt != null) {
        message = message.fgCyan().a("-").a(shortOpt);
        if (abbreviatedLongOpt != null || longOpt != null) {
          message = message.reset().a(",").newline();
        }
      }
      if (abbreviatedLongOpt != null) {
        message = message.fgGreen().a("--").a(abbreviatedLongOpt).reset();
        if (longOpt != null) {
          message = message.reset().a(",").newline();
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
