/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class Main {

  private static final Config REFERENCE = ConfigFactory.defaultReference().getConfig("dsbulk");
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Config SHORTCUTS = ConfigFactory.parseResourcesAnySyntax("shortcuts.conf");
  private static Config DEFAULT = ConfigFactory.load().getConfig("dsbulk");

  public static void main(String[] args) {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    new Main(args);
  }

  public Main(String[] args) {
    String connectorName;
    try {
      if (args.length == 0 || (args[0].equals("help") && args.length == 1)) {
        emitGlobalHelp();
        return;
      }

      if (args[0].equals("help")) {
        emitSectionHelp(args[1]);
        return;
      }

      String[] optionArgs =
          (args[0].startsWith("-")) ? args : Arrays.copyOfRange(args, 1, args.length);
      initDefaultConfig(optionArgs);

      // Figure out connector-name from config + command line.
      connectorName = resolveConnectorName(optionArgs);

      // Parse command line args fully, integrate with default config, and run.
      Config cmdLineConfig = parseCommandLine(connectorName, args[0], optionArgs);
      DefaultLoaderConfig config = new DefaultLoaderConfig(cmdLineConfig.withFallback(DEFAULT));
      config.checkValid(REFERENCE);
      WorkflowType workflowType = WorkflowType.valueOf(args[0].toUpperCase());
      Workflow workflow = workflowType.newWorkflow(config);
      workflow.init();
      workflow.execute();
    } catch (HelpRequestException e) {
      emitGlobalHelp();
    } catch (VersionRequestException e) {
      PrintWriter pw = new PrintWriter(System.out);
      pw.println(getVersionMessage());
      pw.flush();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private static void emitSectionHelp(String sectionName) {
    if (!SettingsDocumentor.GROUPS.containsKey(sectionName)) {
      // Write error message, available group names, raise as error.
      throw new IllegalArgumentException(
          String.format(
              "%s is not a valid section. Available sections include the following:%n    %s",
              sectionName, String.join("\n    ", getGroupNames())));
    }

    String connectorName = null;
    if (sectionName.startsWith("connector.")) {
      connectorName = sectionName.substring(10);
    }
    Options options =
        createOptions(
            SettingsDocumentor.GROUPS.get(sectionName).getSettings(),
            getLongToShortMap(connectorName));
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

  private static void emitGlobalHelp() {
    Options options = createOptions(SettingsDocumentor.COMMON_SETTINGS, getLongToShortMap(null));
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

  @NotNull
  private static Set<String> getGroupNames() {
    Set<String> groupNames = SettingsDocumentor.GROUPS.keySet();
    groupNames.remove("Common");
    return groupNames;
  }

  private static Options createOptions(
      Collection<String> settings, Map<String, String> longToShortOptions) {
    Options options = new Options();

    LoaderConfig config = new DefaultLoaderConfig(DEFAULT);
    for (String setting : settings) {
      options.addOption(
          createOption(config, longToShortOptions, setting, config.getValue(setting)));
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

  private static String resolveConnectorName(String[] optionArgs) throws ParseException {
    String connectorName = DEFAULT.getString("connector.name");
    if (connectorName.isEmpty()) {
      connectorName = null;
    }

    String connectorNameFromArgs = getConnectorNameFromArgs(optionArgs);
    if (connectorNameFromArgs != null && !connectorNameFromArgs.isEmpty()) {
      connectorName = connectorNameFromArgs;
    }
    return connectorName;
  }

  static String getVersionMessage() {
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
    return String.format("DataStax Bulk Loader/Unloader v%s", version);
  }

  static Config parseCommandLine(String connectorName, String subcommand, String[] args)
      throws ParseException, HelpRequestException, VersionRequestException {
    Options options = createOptions(getLongToShortMap(connectorName));

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      // User is asking for help. No real error here, but raising an empty
      // exception gets the job done.
      throw new HelpRequestException();
    }

    if (cmd.hasOption("version")) {
      throw new VersionRequestException();
    }

    if (!Arrays.asList("load", "unload").contains(subcommand)) {
      throw new ParseException(
          "First argument must be subcommand \"load\", \"unload\", or \"help\"");
    }

    Iterator<Option> it = cmd.iterator();
    Config userSettings = ConfigFactory.empty();
    while (it.hasNext()) {
      Option option = it.next();
      if (option.getOpt() != null && option.getOpt().equals("f")) {
        // Skip -f; it doesn't play into this.
        continue;
      }
      String path = option.getLongOpt();
      String value = option.getValue();
      ConfigValueType type = DEFAULT.getValue(path).valueType();
      if (type == ConfigValueType.STRING) {
        value = "\"" + value + "\"";
      }
      userSettings = ConfigFactory.parseString(path + "=" + value).withFallback(userSettings);
    }
    return userSettings;
  }

  private static void initDefaultConfig(String[] optionArgs) {
    // If the user specified the -f option (giving us an app config path),
    // set the config.file property to tell TypeSafeConfig.

    String appConfigPath = getAppConfigPath(optionArgs);
    if (appConfigPath != null) {
      System.setProperty("config.file", appConfigPath);
      ConfigFactory.invalidateCaches();
      DEFAULT = ConfigFactory.load().getConfig("dsbulk");
    }
  }

  private static String getAppConfigPath(String[] optionArgs) {
    // Walk through args, looking for a -f option + value.
    boolean foundDashF = false;
    String appConfigPath = null;
    for (String arg : optionArgs) {
      if (arg.equals("-f")) {
        foundDashF = true;
      } else if (foundDashF) {
        appConfigPath = arg;
        break;
      }
    }

    return appConfigPath;
  }

  private static String getConnectorNameFromArgs(String[] optionArgs) {
    // Walk through args, looking for a -c / --connector.name option + value.
    boolean foundOpt = false;
    String connectorName = null;
    for (String arg : optionArgs) {
      if (arg.equals("-c") || arg.equals("--connector.name")) {
        foundOpt = true;
      } else if (foundOpt) {
        connectorName = arg;
        break;
      }
    }

    return connectorName;
  }

  @NotNull
  private static Map<String, String> getLongToShortMap(String connectorName) {
    Map<String, String> longToShortOptions = new HashMap<>();

    // Add global shortcuts first
    for (Map.Entry<String, ConfigValue> entry :
        SHORTCUTS.getConfig("dsbulk.shortcuts").entrySet()) {
      longToShortOptions.put(entry.getValue().unwrapped().toString(), entry.getKey());
    }

    // Add connector-specific entries next. If there's overlap of shortcuts, log a warning.
    if (connectorName != null && SHORTCUTS.hasPath("dsbulk." + connectorName + "-shortcuts")) {
      for (Map.Entry<String, ConfigValue> entry :
          SHORTCUTS.getConfig("dsbulk." + connectorName + "-shortcuts").entrySet()) {
        String longOption = entry.getValue().unwrapped().toString();
        String shortOption = entry.getKey();
        if (longToShortOptions.containsKey(longOption)
            || longToShortOptions.containsValue(shortOption)) {
          LOGGER.warn(
              String.format(
                  "Shortcut %s => %s in %s shortcuts overlaps with global shortcuts and will be ignored",
                  shortOption, longOption, connectorName));
          continue;
        }
        longToShortOptions.put(longOption, shortOption);
      }
    }
    return longToShortOptions;
  }

  private static Options createOptions(Map<String, String> longToShortOptions) {
    Options options = new Options();

    LoaderConfig config = new DefaultLoaderConfig(DEFAULT);

    for (Map.Entry<String, ConfigValue> entry : DEFAULT.entrySet()) {
      String longName = entry.getKey();
      Option option = createOption(config, longToShortOptions, longName, entry.getValue());
      options.addOption(option);
    }

    // Add the --help, --version, -f options
    options.addOption(
        null,
        "help",
        false,
        "This help text. May be combined with -c <connectorName> to see short options for a "
            + "particular connector");
    options.addOption(null, "version", false, "Print out the version of this tool.");
    options.addOption(SettingsDocumentor.CONFIG_FILE_OPTION);
    return options;
  }

  private static Option createOption(
      LoaderConfig config,
      Map<String, String> longToShortOptions,
      String longName,
      ConfigValue value) {
    Option.Builder option;
    String shortName = longToShortOptions.get(longName);
    if (shortName == null) {
      option = Option.builder();
    } else {
      option = Option.builder(shortName);
    }
    option
        .hasArg()
        .longOpt(longName)
        .argName(config.getTypeString(longName))
        .desc(getSanitizedDescription(longName, value));
    return option.build();
  }

  private static String getSanitizedDescription(String longName, ConfigValue value) {
    String desc =
        DEFAULT.getValue(longName).origin().comments().stream().collect(Collectors.joining("\n"));

    // The description is a little dirty.
    // * Replace consecutive spaces with a single space.
    // * Remove **'s, which have meaning in markdown but not useful here. However,
    //   we do have a legit case of ** when describing file patterns (e.g. **/*.csv).
    //   Those sorts of instances are preceded by ", so don't replace those.

    desc = desc.replaceAll(" +", " ").replaceAll("([^\"])\\*\\*", "$1").trim();
    desc += "\nDefaults to " + value.render(ConfigRenderOptions.concise()) + ".";
    return desc;
  }

  // Simple exception indicating that the user wants to know the
  // version of the tool.
  private static class VersionRequestException extends Exception {}

  // Simple exception indicating that the user wants the main help output.
  private static class HelpRequestException extends Exception {}
}
