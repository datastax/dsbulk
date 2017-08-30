/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import com.datastax.dsbulk.commons.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class Main {

  private static final Config REFERENCE = ConfigFactory.defaultReference().getConfig("dsbulk");
  private static final Config DEFAULT = ConfigFactory.load().getConfig("dsbulk");
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Config SHORTCUTS = ConfigFactory.parseResourcesAnySyntax("shortcuts.conf");
  private final String version;

  public static void main(String[] args) {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    new Main(args);
  }

  public Main(String[] args) {
    String connectorName = DEFAULT.getString("connector.name");
    if (connectorName.isEmpty()) {
      connectorName = null;
    }

    version =
        new BufferedReader(
                new InputStreamReader(
                    Main.class.getClassLoader().getResourceAsStream("version.txt")))
            .lines()
            .collect(Collectors.joining("\n"));

    try {
      if (args.length == 0) {
        throw new ParseException("First argument must be subcommand \"load\" or \"unload\"");
      }

      String[] optionArgs =
          (args[0].startsWith("-")) ? args : Arrays.copyOfRange(args, 1, args.length);

      String connectorNameFromArgs = getConnectorNameFromArgs(optionArgs);
      if (connectorNameFromArgs != null && !connectorNameFromArgs.isEmpty()) {
        connectorName = connectorNameFromArgs;
      }

      Config cmdLineConfig = parseCommandLine(connectorName, args[0], optionArgs);
      DefaultLoaderConfig config = new DefaultLoaderConfig(cmdLineConfig.withFallback(DEFAULT));
      config.checkValid(REFERENCE);
      WorkflowType workflowType = WorkflowType.valueOf(args[0].toUpperCase());
      Workflow workflow = workflowType.newWorkflow(config);
      workflow.init();
      workflow.execute();
    } catch (VersionRequestException e) {
      System.out.println(getVersionMessage());
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setOptionComparator(Comparator.comparing(Option::getLongOpt));

      PrintWriter pw = new PrintWriter(System.err);
      Options options = createOptions(connectorName);
      String footer =
          "NOTE: short options for some connectors may not be shown. "
              + "Run with \"--help -c <connector-name>\" to see the short options available for "
              + "those connectors.";
      pw.println(getVersionMessage());
      formatter.printHelp(
          pw, 150, "dsbulk (load|unload) [options]", "options:", options, 0, 5, footer);
      pw.println(e.getMessage());
      pw.flush();
    }
  }

  private String getVersionMessage() {
    return String.format("DataStax Bulk Loader/Unloader v%s", version);
  }

  private String getConnectorNameFromArgs(String[] args) throws ParseException {
    String connectorName = null;
    Options basicOptions = new Options();
    basicOptions.addOption(Option.builder("c").hasArg().longOpt("connector.name").build());

    String[] remainingArgs = args;
    CommandLineParser parser = new DefaultParser();
    while (remainingArgs.length > 0) {
      CommandLine cmd = parser.parse(basicOptions, remainingArgs, true);

      if (cmd.hasOption("connector.name")) {
        connectorName = cmd.getOptionValue("connector.name");
        break;
      }

      // Not found. Could be that we're choking on one of the earlier args in the arg list.
      // Skip it and try again.
      remainingArgs = Arrays.copyOfRange(cmd.getArgs(), 1, cmd.getArgs().length);
    }
    return connectorName;
  }

  private static Options createOptions(String connectorName) {
    Map<String, String> longToShortOptions = new HashMap<>();

    // Add global shortcuts first
    for (Map.Entry<String, ConfigValue> entry :
        SHORTCUTS.getConfig("dsbulk.shortcuts").entrySet()) {
      longToShortOptions.put(entry.getValue().unwrapped().toString(), entry.getKey());
    }

    // Add connector-specific entries next. If there's overlap of shortcuts, log a warning.
    if (connectorName != null) {
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

    Options options = new Options();

    // special-case options of type OBJECT as these do not show up in entrySet()
    // FIXME maybe we should transform these in regular STRING values and parse the string in SchemaSettings
    // As a bonus, we could get rid of enclosing braces, e.g.
    // schema.mapping = { fieldA = col1, fieldB = col2 }
    // would become:
    // schema.mapping = "fieldA = col1, fieldB = col2"
    Option schemaMapping =
        createOption(
            longToShortOptions, "schema.mapping", DEFAULT.getConfig("schema.mapping").root());
    options.addOption(schemaMapping);
    Option recordMetadata =
        createOption(
            longToShortOptions,
            "schema.recordMetadata",
            DEFAULT.getConfig("schema.recordMetadata").root());
    options.addOption(recordMetadata);
    for (Map.Entry<String, ConfigValue> entry : DEFAULT.entrySet()) {
      String longName = entry.getKey();
      Option option = createOption(longToShortOptions, longName, entry.getValue());
      options.addOption(option);
    }

    // Add the --help and --version options
    options.addOption(
        null,
        "help",
        false,
        "This help text. May be combined with -c <connectorName> to see short options for a "
            + "particular connector");
    options.addOption(null, "version", false, "Print out the version of this tool.");
    return options;
  }

  private static Option createOption(
      Map<String, String> longToShortOptions, String longName, ConfigValue value) {
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
        .argName(getArgName(longName, value.valueType()))
        .desc(getSanitizedDescription(longName, value));
    return option.build();
  }

  static Config parseCommandLine(String connectorName, String subcommand, String[] args)
      throws ParseException, VersionRequestException {
    Options options = createOptions(connectorName);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      // User is asking for help. No real error here, but raising an empty
      // exception gets the job done.
      throw new ParseException("");
    }

    if (cmd.hasOption("version")) {
      throw new VersionRequestException();
    }

    if (!Arrays.asList("load", "unload").contains(subcommand)) {
      throw new ParseException("First argument must be subcommand \"load\" or \"unload\"");
    }

    Iterator<Option> it = cmd.iterator();
    Config userSettings = ConfigFactory.empty();
    while (it.hasNext()) {
      Option option = it.next();
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

  private static String getArgName(String longName, ConfigValueType type) {
    switch (type) {
      case STRING:
        return "string";
      case LIST:
        ConfigList list = DEFAULT.getList(longName);
        if (list.isEmpty()) {
          return "list";
        } else {
          return "list<" + getArgName(null, list.get(0).valueType()) + ">";
        }
      case NUMBER:
        return "number";
      case BOOLEAN:
        return "boolean";
    }
    return "arg";
  }

  private static String getSanitizedDescription(String longName, ConfigValue value) {
    String desc =
        DEFAULT.getValue(longName).origin().comments().stream().collect(Collectors.joining("\n"));
    desc = desc.replaceAll(" +", " ").trim();
    desc += "\nDefaults to " + value.render(ConfigRenderOptions.concise()) + ".";
    return desc;
  }

  // Simple exception indicating that the user wants to know the
  // version of the tool.
  private static class VersionRequestException extends Exception {}
}
