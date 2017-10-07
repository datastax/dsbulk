/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.engine.internal.OptionUtils.DEFAULT;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.engine.internal.HelpUtils;
import com.datastax.dsbulk.engine.internal.OptionUtils;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueType;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class Main {

  private static final Config REFERENCE = ConfigFactory.defaultReference().getConfig("dsbulk");
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
    String connectorName;
    try {
      if (args.length == 0 || (args[0].equals("help") && args.length == 1)) {
        HelpUtils.emitGlobalHelp();
        return args.length == 0 ? 1 : 0;
      }

      if (args[0].equals("help")) {
        HelpUtils.emitSectionHelp(args[1]);
        return 0;
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
      WorkflowThread workflowThread = new WorkflowThread(workflowType, config);
      Runtime.getRuntime().addShutdownHook(new CleanupThread(workflowThread));
      workflowThread.start();
      workflowThread.join();
      return 0;
    } catch (HelpRequestException e) {
      HelpUtils.emitGlobalHelp();
      return 0;
    } catch (VersionRequestException e) {
      PrintWriter pw = new PrintWriter(System.out);
      pw.println(HelpUtils.getVersionMessage());
      pw.flush();
      return 0;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return 1;
    } catch (Error e) {
      LOGGER.error(e.getMessage(), e);
      throw e;
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

  static Config parseCommandLine(String connectorName, String subcommand, String[] args)
      throws ParseException, HelpRequestException, VersionRequestException {
    Options options = OptionUtils.createOptions(connectorName);

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
      if (!foundDashF && arg.equals("-f")) {
        foundDashF = true;
      } else if (foundDashF) {
        appConfigPath = arg;
        break;
      }
    }
    return appConfigPath;
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

  private static class WorkflowThread extends Thread {

    private final WorkflowType workflowType;
    private final LoaderConfig config;

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
        workflow.execute();
      } catch (Throwable t) {
        // Reactor framework often wraps InterruptedException
        Throwable root = Throwables.getRootCause(t);
        if (t instanceof InterruptedException || root instanceof InterruptedException) {
          LOGGER.error(workflow + " aborted.", t);
          Thread.currentThread().interrupt();
        } else {
          LOGGER.error(workflow + " failed.", t);
        }
        if (t instanceof Error) {
          throw ((Error) t);
        }
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
  private static class HelpRequestException extends Exception {}
}
