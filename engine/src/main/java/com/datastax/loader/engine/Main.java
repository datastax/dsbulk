/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.url.LoaderURLStreamHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
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

/** */
public class Main {

  private static final Config REFERENCE = ConfigFactory.defaultReference().getConfig("datastax-loader");
  private static final Config DEFAULT = ConfigFactory.load().getConfig("datastax-loader");

  public static void main(String[] args) {
    new Main(args);
  }

  public Main(String[] args) {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    Options options = createOptions();
    try {
      Config cmdLineConfig = parseCommandLine(options, args);
      DefaultLoaderConfig config = new DefaultLoaderConfig(cmdLineConfig.withFallback(DEFAULT));
      config.checkValid(REFERENCE);
      WorkflowType workflowType = WorkflowType.valueOf(args[0].toUpperCase());
      Workflow workflow = workflowType.newWorkflow(config);
      workflow.init();
      workflow.execute();
    } catch (Exception e) {
      e.printStackTrace();
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(System.err);
      formatter.printHelp(
        pw, 150, "datastax-loader (read|write) [options]", "options:", options, 0, 5, "", true);
    }
  }

  private static Options createOptions() {
    Options options = new Options();
    Set<String> shortNames = new HashSet<>();
    // special-case options of type OBJECT as these do not show up in entrySet()
    // FIXME maybe we should transform these in regular STRING values and parse the string in SchemaSettings
    // As a bonus, we could get rid of enclosing braces, e.g.
    // schema.mapping = { fieldA = col1, fieldB = col2 }
    // would become:
    // schema.mapping = "fieldA = col1, fieldB = col2"
    Option schemaMapping =
      createOption(shortNames, "schema.mapping", DEFAULT.getConfig("schema.mapping").root());
    options.addOption(schemaMapping);
    Option recordMetadata =
        createOption(shortNames, "schema.recordMetadata", DEFAULT.getConfig("schema.recordMetadata").root());
    options.addOption(recordMetadata);
    for (Map.Entry<String, ConfigValue> entry : DEFAULT.entrySet()) {
      String longName = entry.getKey();
      Option option = createOption(shortNames, longName, entry.getValue());
      options.addOption(option);
    }
    return options;
  }

  private static Option createOption(Set<String> shortNames, String longName, ConfigValue value) {
    String shortName = createShortName(longName, shortNames);
    Option.Builder option;
    if (shortName == null) {
      option = Option.builder();
    } else {
      option = Option.builder(shortName);
    }
    option.hasArg();
    option.longOpt(longName);
    option.argName(getArgName(longName, value.valueType()));
    option.desc(getSanitizedDescription(longName, value));
    return option.build();
  }

  private static Config parseCommandLine(Options options, String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
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

  private static String createShortName(String longName, Set<String> shortNames) {
    String[] tokens = longName.split("(\\.|(?=[A-Z]))");
    String shortName = tentativeShortName(tokens);
    if (shortNames.contains(shortName)) {
      return null;
    }
    shortNames.add(shortName);
    return shortName;
  }

  private static String tentativeShortName(String[] tokens) {
    StringBuilder sb = new StringBuilder();
    for (String token : tokens) {
      sb.append(token.substring(0, 1).toLowerCase());
    }
    return sb.toString();
  }
}
