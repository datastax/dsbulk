/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.engine.internal.log.TooManyErrorsException;
import com.datastax.dsbulk.engine.internal.utils.HelpUtils;
import com.datastax.dsbulk.engine.internal.utils.OptionUtils;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigValueType;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Config REFERENCE = ConfigFactory.defaultReference().getConfig("dsbulk");
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static final int STATUS_OK = 0;
  public static final int STATUS_COMPLETED_WITH_ERRORS = 1;
  public static final int STATUS_ABORTED_TOO_MANY_ERRORS = 2;
  public static final int STATUS_ABORTED_FATAL_ERROR = 3;
  public static final int STATUS_INTERRUPTED = 4;
  public static final int STATUS_CRASHED = 5;

  // Maybe be overridden to handle the "-f" override for application.conf.
  public static Config DEFAULT = ConfigFactory.load().getConfig("dsbulk");

  private final String[] args;

  public static void main(String[] args) {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    int status = new Main(args).run();
    System.exit(status);
  }

  public Main(String[] args) {
    this.args = args;
  }

  public int run() {
    try {
      // The first arg can be a subcommand or option...or no arg. We want to treat these
      // cases as follows:
      // no arg: same as help subcommand with no connectorName name.
      // first arg that is a short/long option: all args are short/long options (no subcommand).
      // first arg is not a short/long option: first arg is a subcommand, rest are options.

      String[] optionArgs;
      String subCommand = null;
      if (args.length == 0) {
        subCommand = "help";
        optionArgs = new String[] {};
      } else if (args[0].startsWith("-")) {
        optionArgs = args;
      } else {
        subCommand = args[0];
        optionArgs = Arrays.copyOfRange(args, 1, args.length);
      }

      initDefaultConfig(optionArgs);

      // Parse command line args fully, integrate with default config, and run.
      Config cmdLineConfig = parseCommandLine(subCommand, optionArgs);
      DefaultLoaderConfig config = new DefaultLoaderConfig(cmdLineConfig.withFallback(DEFAULT));
      config.checkValid(REFERENCE);
      WorkflowType workflowType = WorkflowType.valueOf(args[0].toUpperCase());
      WorkflowThread workflowThread = new WorkflowThread(workflowType, config);
      Runtime.getRuntime().addShutdownHook(new CleanupThread(workflowThread));
      workflowThread.start();
      workflowThread.join();
      return workflowThread.status;
    } catch (GlobalHelpRequestException e) {
      HelpUtils.emitGlobalHelp(e.getConnectorName());
      return STATUS_OK;
    } catch (SectionHelpRequestException e) {
      try {
        HelpUtils.emitSectionHelp(e.getSectionName());
        return STATUS_OK;
      } catch (Exception e2) {
        LOGGER.error(e2.getMessage(), e2);
        return STATUS_CRASHED;
      }
    } catch (VersionRequestException e) {
      // Use the OS charset
      PrintWriter pw =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(System.out, Charset.defaultCharset())));
      pw.println(HelpUtils.getVersionMessage());
      pw.flush();
      return STATUS_OK;
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      return STATUS_CRASHED;
    }
  }

  private static String getConnectorNameFromArgs(String[] optionArgs) {
    // Walk through args, looking for a -c / --connector.name option + value.
    boolean foundOpt = false;
    String connectorName = null;
    for (String arg : optionArgs) {
      if (arg.equals("-c") || arg.equals("--connector.name")) {
        foundOpt = true;
      } else if (arg.startsWith("--connector.name=")) {
        connectorName = arg.substring("--connector.name=".length());
        break;
      } else if (foundOpt) {
        connectorName = arg;
        break;
      }
    }

    return connectorName;
  }

  static Config parseCommandLine(String subCommand, @NotNull String[] args)
      throws ParseException, GlobalHelpRequestException, SectionHelpRequestException,
          VersionRequestException {
    // Figure out connector-name from config + command line.
    String connectorName = resolveConnectorName(args);
    Options options = OptionUtils.createOptions(connectorName);

    CommandLineParser parser = new CmdlineParser();
    CommandLine cmd = parser.parse(options, args);
    List<String> remainingArgs = cmd.getArgList();
    String maybeSection = remainingArgs.isEmpty() ? null : remainingArgs.get(0);

    if (cmd.hasOption("help") || "help".equals(subCommand)) {
      // User is asking for help.
      if (maybeSection != null) {
        throw new SectionHelpRequestException(maybeSection);
      } else {
        throw new GlobalHelpRequestException(cmd.getOptionValue('c'));
      }
    }

    if (cmd.hasOption("version")) {
      throw new VersionRequestException();
    }

    if (!Arrays.asList("load", "unload").contains(subCommand)) {
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
      try {
        // all user input is expected to be already valid HOCON;
        // however we accept a relaxed syntax for strings, lists and maps.
        String formatted = value;
        if (type == ConfigValueType.STRING) {
          // if the user did not surround the string with double-quotes, do it for him.
          formatted = ConfigUtils.ensureQuoted(value);
        } else if (type == ConfigValueType.LIST) {
          // if the user did not surround the list elements with square brackets, do it for him.
          formatted = ConfigUtils.ensureBrackets(value);
        } else if (type == ConfigValueType.OBJECT) {
          // if the user did not surround the map entries with curly braces, do it for him.
          formatted = ConfigUtils.ensureBraces(value);
        }
        userSettings =
            ConfigFactory.parseString(
                    path + "=" + formatted,
                    ConfigParseOptions.defaults().setOriginDescription(path))
                .withFallback(userSettings);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        throw new IllegalArgumentException(
            String.format("%s: Expecting %s, got '%s'", path, type, value), e);
      }
    }
    return userSettings;
  }

  private static void initDefaultConfig(String[] optionArgs) {
    // If the user specified the -f option (giving us an app config path),
    // set the config.file property to tell TypeSafeConfig.
    Path appConfigPath = getAppConfigPath(optionArgs);
    if (appConfigPath != null) {
      System.setProperty("config.file", appConfigPath.toString());
      ConfigFactory.invalidateCaches();
      try {
        DEFAULT = ConfigFactory.load().getConfig("dsbulk");
      } catch (ConfigException.Parse e) {
        LOGGER.error(e.getMessage(), e);
        throw new IllegalArgumentException(
            String.format(
                "Error parsing configuration file %s. "
                    + "Please make sure its format is compliant with HOCON syntax. "
                    + "If you are using \\ (backslash) to define a path, "
                    + "escape it with \\\\ or use / (forward slash) instead.",
                appConfigPath),
            e);
      }
    }
    ConfigFactory.invalidateCaches();
    DEFAULT = ConfigFactory.load().getConfig("dsbulk");
  }

  private static Path getAppConfigPath(String[] optionArgs) {
    // Walk through args, looking for a -f option + value.
    boolean foundDashF = false;
    Path appConfigPath = null;
    for (String arg : optionArgs) {
      if (!foundDashF && arg.equals("-f")) {
        foundDashF = true;
      } else if (foundDashF) {
        appConfigPath = ConfigUtils.resolvePath(arg);
        break;
      }
    }
    return appConfigPath;
  }

  private static String resolveConnectorName(String[] optionArgs) {
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

  private static class WorkflowThread extends Thread {

    private final WorkflowType workflowType;
    private final LoaderConfig config;
    private int status = -1;

    private WorkflowThread(WorkflowType workflowType, LoaderConfig config) {
      super("workflow-runner");
      this.workflowType = workflowType;
      this.config = config;
    }

    @Override
    public void run() {
      Workflow workflow = workflowType.newWorkflow(config);
      try {
        workflow.init();
        status = workflow.execute() ? STATUS_OK : STATUS_COMPLETED_WITH_ERRORS;
      } catch (TooManyErrorsException e) {
        LOGGER.error(workflow + " aborted: " + e.getMessage(), e);
        status = STATUS_ABORTED_TOO_MANY_ERRORS;
      } catch (Throwable t) {
        // Reactor framework often wraps InterruptedException
        Throwable root = Throwables.getRootCause(t);
        if (t instanceof InterruptedException || root instanceof InterruptedException) {
          status = STATUS_INTERRUPTED;
          LOGGER.error(workflow + " interrupted.", t);
          // do not set the thread's interrupted status, we are going to exit anyway
        } else if (t instanceof Exception) {
          status = STATUS_ABORTED_FATAL_ERROR;
          LOGGER.error(workflow + " aborted: " + t.getMessage(), t);
        } else {
          status = STATUS_CRASHED;
          LOGGER.error(workflow + " failed unexpectedly: " + t.getMessage(), t);
        }
        // make sure the error above is printed to System.err
        // before the closing sequence is printed to System.out
        System.err.flush();
      } finally {
        try {
          workflow.close();
        } catch (Exception e) {
          LOGGER.error(String.format("%s could not be closed.", workflow), e);
        }
      }
    }
  }

  private static class CleanupThread extends Thread {

    private final WorkflowThread workflowThread;

    private CleanupThread(WorkflowThread workflowThread) {
      super("cleanup=thread");
      this.workflowThread = workflowThread;
    }

    @Override
    public void run() {
      try {
        if (workflowThread.isAlive()) {
          workflowThread.interrupt();
          workflowThread.join();
        }
      } catch (Exception ignored) {
      }
    }
  }

  // Simple exception indicating that the user wants to know the
  // version of the tool.
  private static class VersionRequestException extends Exception {}

  // Simple exception indicating that the user wants the main help output.
  private static class GlobalHelpRequestException extends Exception {
    private final String connectorName;

    GlobalHelpRequestException(String connectorName) {
      this.connectorName = connectorName;
    }

    String getConnectorName() {
      return connectorName;
    }
  }

  // Simple exception indicating that the user wants the help for a particular section.
  private static class SectionHelpRequestException extends Exception {
    private final String sectionName;

    SectionHelpRequestException(String sectionName) {
      this.sectionName = sectionName;
    }

    String getSectionName() {
      return sectionName;
    }
  }

  /**
   * Commons-cli parser that errors out when attempting to interpret a short option that has a value
   * concatenated to it. We don't want to support that kind of usage.
   *
   * <p>Motivating example: User says `-hdr` instead of `-header`. `-h` is a valid option, so dsbulk
   * interprets it as the `-h` option with value `dr`, which is not the user's intention.
   */
  private static class CmdlineParser extends DefaultParser {
    @Override
    protected void handleConcatenatedOptions(String token) throws ParseException {
      throw new UnrecognizedOptionException("Unrecognized option: " + token, token);
    }
  }
}
