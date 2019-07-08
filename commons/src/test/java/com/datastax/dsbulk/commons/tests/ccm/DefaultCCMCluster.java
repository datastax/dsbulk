/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.OSS;
import static com.datastax.dsbulk.commons.tests.utils.NetworkUtils.DEFAULT_IP_PREFIX;
import static com.datastax.dsbulk.commons.tests.utils.NetworkUtils.findAvailablePort;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.commons.tests.utils.MemoryUtils;
import com.datastax.dsbulk.commons.tests.utils.NetworkUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCCMCluster implements CCMCluster {

  static final Type CCM_TYPE;
  static final Version CCM_VERSION;

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCCMCluster.class);
  private static final Logger CCM_OUT_LOGGER =
      LoggerFactory.getLogger("com.datastax.dsbulk.commons.tests.ccm.CCM_OUT");
  private static final Logger CCM_ERR_LOGGER =
      LoggerFactory.getLogger("com.datastax.dsbulk.commons.tests.ccm.CCM_ERR");

  public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
  private static final String DEFAULT_SERVER_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  private static final String DEFAULT_SERVER_KEYSTORE_PASSWORD = "cassandra1sfun";

  public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";
  public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";
  public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE = createTempStore("/client.truststore");
  public static final File DEFAULT_CLIENT_KEYSTORE_FILE = createTempStore("/client.keystore");

  // Contain the same keypair as the client keystore, but in format usable by OpenSSL
  public static final String DEFAULT_CLIENT_CERT_CHAIN_PATH = "/client.crt";
  public static final String DEFAULT_CLIENT_PRIVATE_KEY_PATH = "/client.key";
  public static final File DEFAULT_CLIENT_PRIVATE_KEY_FILE = createTempStore("/client.key");
  public static final File DEFAULT_CLIENT_CERT_CHAIN_FILE = createTempStore("/client.crt");
  private static final File DEFAULT_SERVER_TRUSTSTORE_FILE = createTempStore("/server.truststore");
  private static final File DEFAULT_SERVER_KEYSTORE_FILE = createTempStore("/server.keystore");

  // major DSE versions
  private static final Version V6_0_0 = Version.parse("6.0.0");
  private static final Version V5_1_0 = Version.parse("5.1.0");
  private static final Version V5_0_0 = Version.parse("5.0.0");
  private static final Version V4_8_0 = Version.parse("4.8.0");
  private static final Version V4_7_0 = Version.parse("4.7.0");
  private static final Version V4_6_0 = Version.parse("4.6.0");

  // mapped C* versions from DSE versions
  private static final Version V4_0_0 = Version.parse("4.0.0");
  private static final Version V3_11_2 = Version.parse("3.11.2");
  private static final Version V3_10 = Version.parse("3.10");
  private static final Version V3_0_15 = Version.parse("3.0.15");
  private static final Version V2_2_0 = Version.parse("2.2.0");
  private static final Version V2_1_19 = Version.parse("2.1.19");
  private static final Version V2_1_11 = Version.parse("2.1.11");
  private static final Version V2_0_14 = Version.parse("2.0.14");

  /** The install arguments to pass to CCM when creating the cluster. */
  private static final Set<String> DEFAULT_CREATE_OPTIONS;

  private static final Pattern DATACENTER_PATTERN =
      Pattern.compile("^Datacenter: (\\w+)$", Pattern.MULTILINE);

  /**
   * The environment variables to use when invoking CCM. Inherits the current processes environment,
   * but will also prepend to the PATH variable the value of the 'ccm.path' property and set
   * JAVA_HOME variable to the 'com.datastax.dsbulk.commons.tests.ccm.JAVA_HOME' variable.
   */
  private static final Map<String, String> ENVIRONMENT_MAP;

  /** The command to use to launch CCM */
  private static final String CCM_COMMAND;

  static {
    boolean dse =
        Boolean.parseBoolean(
            System.getProperty("com.datastax.dsbulk.commons.tests.ccm.CCM_IS_DSE", "false"));
    boolean ddac =
        Boolean.parseBoolean(
            System.getProperty("com.datastax.dsbulk.commons.tests.ccm.CCM_IS_DDAC", "true"));
    CCM_TYPE = dse ? DSE : (ddac ? DDAC : OSS);
    String versionStr = System.getProperty("com.datastax.dsbulk.commons.tests.ccm.CCM_VERSION");
    CCM_VERSION = Version.parse(versionStr == null ? CCM_TYPE.getDefaultVersion() : versionStr);
    LOGGER.info("CCM tests configured to use {} version {}", CCM_TYPE, CCM_VERSION);
    String installDirectory =
        System.getProperty("com.datastax.dsbulk.commons.tests.ccm.CCM_DIRECTORY");
    String branch = System.getProperty("com.datastax.dsbulk.commons.tests.ccm.CCM_BRANCH");

    Set<String> defaultCreateOptions = new LinkedHashSet<>();
    if (installDirectory != null && !installDirectory.trim().isEmpty()) {
      defaultCreateOptions.add("--install-dir=" + new File(installDirectory).getAbsolutePath());
    } else if (branch != null && !branch.trim().isEmpty()) {
      defaultCreateOptions.add("-v git:" + branch.trim().replaceAll("\"", ""));
    } else {
      defaultCreateOptions.add("-v " + CCM_VERSION);
    }
    defaultCreateOptions.add(CCM_TYPE.getCreateOption());
    DEFAULT_CREATE_OPTIONS = Collections.unmodifiableSet(defaultCreateOptions);

    // Inherit the current environment.
    Map<String, String> envMap = new HashMap<>(new ProcessBuilder().environment());
    // If ccm path is set, override the PATH variable with it.
    String ccmPath = System.getProperty("com.datastax.dsbulk.commons.tests.ccm.PATH");
    if (ccmPath != null) {
      String existingPath = envMap.get("PATH");
      if (existingPath == null) {
        existingPath = "";
      }
      envMap.put("PATH", ccmPath + File.pathSeparator + existingPath);
    }
    // If ccm Java home is set, override the JAVA_HOME variable with it.
    String ccmJavaHome = System.getProperty("com.datastax.dsbulk.commons.tests.ccm.JAVA_HOME");
    if (ccmJavaHome != null) {
      envMap.put("JAVA_HOME", ccmJavaHome);
    }
    ENVIRONMENT_MAP = Collections.unmodifiableMap(envMap);

    if (PlatformUtils.isWindows()) {
      CCM_COMMAND = "cmd /c ccm.py";
    } else {
      CCM_COMMAND = "ccm";
    }
  }

  private final String clusterName;
  private final Type clusterType;
  private final Version version;
  private final Version cassandraVersion;
  private final int[] nodesPerDC;
  private final int storagePort;
  private final int thriftPort;
  private final int binaryPort;
  private final File ccmDir;
  private final String jvmArgs;
  private volatile boolean keepLogs = false;

  private volatile State state = State.CREATED;

  private DefaultCCMCluster(
      String clusterName,
      CCMCluster.Type clusterType,
      Version version,
      Version cassandraVersion,
      int[] nodesPerDC,
      int binaryPort,
      int thriftPort,
      int storagePort,
      String jvmArgs) {
    this.clusterName = clusterName;
    this.clusterType = clusterType;
    this.version = version;
    this.cassandraVersion = cassandraVersion;
    this.nodesPerDC = nodesPerDC;
    this.storagePort = storagePort;
    this.thriftPort = thriftPort;
    this.binaryPort = binaryPort;
    this.jvmArgs = jvmArgs;
    this.ccmDir = Files.createTempDir();
  }

  /**
   * Creates a new {@link Builder builder} for {@link DefaultCCMCluster} instances.
   *
   * @return a new {@link Builder builder} for {@link DefaultCCMCluster} instances.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static File createTempStore(String storePath) {
    File f = null;
    Closer closer = Closer.create();
    try {
      InputStream trustStoreIs = DefaultCCMCluster.class.getResourceAsStream(storePath);
      closer.register(trustStoreIs);
      f = File.createTempFile("server", ".store");
      LOGGER.debug("Created store file {} for {}.", f, storePath);
      OutputStream trustStoreOs = new FileOutputStream(f);
      closer.register(trustStoreOs);
      ByteStreams.copy(trustStoreIs, trustStoreOs);
    } catch (IOException e) {
      LOGGER.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOGGER.warn("Failure closing streams.", e);
      }
    }
    return f;
  }

  @Override
  public String getClusterName() {
    return clusterName;
  }

  @Override
  public Type getClusterType() {
    return clusterType;
  }

  @Override
  public InetSocketAddress addressOfNode(int node) {
    return new InetSocketAddress(NetworkUtils.addressOfNode(DEFAULT_IP_PREFIX, node), binaryPort);
  }

  @Override
  public InetSocketAddress addressOfNode(int dc, int node) {
    return new InetSocketAddress(
        NetworkUtils.addressOfNode(DEFAULT_IP_PREFIX, nodesPerDC, dc, node), binaryPort);
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public Version getCassandraVersion() {
    return cassandraVersion;
  }

  @Override
  public File getCcmDir() {
    return ccmDir;
  }

  @Override
  public File getClusterDir() {
    return new File(ccmDir, clusterName);
  }

  @Override
  public File getNodeDir(int n) {
    return new File(getClusterDir(), "node" + n);
  }

  @Override
  public File getNodeConfDir(int n) {
    return new File(getNodeDir(n), "conf");
  }

  @Override
  public int getStoragePort() {
    return storagePort;
  }

  @Override
  public int getThriftPort() {
    return thriftPort;
  }

  @Override
  public int getBinaryPort() {
    return binaryPort;
  }

  @Override
  public String getIpPrefix() {
    return DEFAULT_IP_PREFIX;
  }

  @Override
  public List<InetAddress> getInitialContactPoints() {
    return NetworkUtils.allContactPoints(DEFAULT_IP_PREFIX, nodesPerDC);
  }

  @Override
  public void setKeepLogs() {
    this.keepLogs = true;
  }

  @Override
  public synchronized void start() {
    if (state.canTransitionTo(State.STARTED)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Starting: {} - free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      try {
        execute(CCM_COMMAND + " start " + jvmArgs);
        LOGGER.debug("Waiting for binary protocol to show up");
        for (InetAddress node : getInitialContactPoints()) {
          NetworkUtils.waitUntilPortIsUp(new InetSocketAddress(node, getBinaryPort()));
        }
      } catch (CCMException e) {
        LOGGER.error("Could not start " + this, e);
        handleCCMException(e);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Started: {} - Free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      state = State.STARTED;
    }
  }

  @Override
  public synchronized void stop() {
    if (state.canTransitionTo(State.STOPPED)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stopping: {} - free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      try {
        execute(CCM_COMMAND + " stop");
      } catch (CCMException e) {
        LOGGER.error("Could not stop " + this, e);
        handleCCMException(e);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stopped: {} - free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      state = State.STOPPED;
    }
  }

  @Override
  public synchronized void close() {
    stop();
  }

  @Override
  public synchronized void forceStop() {
    if (state.canTransitionTo(State.STOPPED)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Force stopping: {} - free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      try {
        execute(CCM_COMMAND + " stop --not-gently");
      } catch (CCMException e) {
        LOGGER.error("Could not force stop " + this, e);
        handleCCMException(e);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stopped: {} - free memory: {} MB", this, MemoryUtils.getFreeMemoryMB());
      }
      state = State.STOPPED;
    }
  }

  @Override
  public synchronized void remove() {
    if (state.canTransitionTo(State.REMOVED)) {
      if (keepLogs) {
        LOGGER.debug("Error during tests, C* logs will be kept in {}", getCcmDir());
      } else {
        LOGGER.debug("Removing: {}", this);
        try {
          execute(CCM_COMMAND + " remove");
        } catch (CCMException e) {
          LOGGER.error("Could not remove " + this, e);
          handleCCMException(e);
        } finally {
          FileUtils.deleteDirectory(getCcmDir().toPath());
        }
        LOGGER.debug("Removed: {}", this);
      }
      state = State.REMOVED;
    }
  }

  @Override
  public String checkForErrors() {
    LOGGER.debug("Checking for errors in: {}", this);
    try {
      return execute(CCM_COMMAND + " checklogerror");
    } catch (CCMException e) {
      LOGGER.warn("Check for errors failed");
      return null;
    }
  }

  @Override
  public synchronized void start(int node) {
    LOGGER.debug(
        String.format(
            "Starting: node %s (%s%s:%s) in %s", node, DEFAULT_IP_PREFIX, node, binaryPort, this));
    try {
      execute(
          CCM_COMMAND + " node%d start --wait-other-notice --wait-for-binary-proto" + jvmArgs,
          node);
    } catch (CCMException e) {
      LOGGER.error(String.format("Could not start node %s in %s", node, this), e);
      LOGGER.error("CCM output:\n{}", e.getOut());
      setKeepLogs();
      String errors = checkForErrors();
      if (errors != null) {
        LOGGER.error("CCM check errors:\n{}", errors);
      }
      throw e;
    }
  }

  @Override
  public synchronized void stop(int node) {
    LOGGER.debug(
        String.format(
            "Stopping: node %s (%s%s:%s) in %s", node, DEFAULT_IP_PREFIX, node, binaryPort, this));
    execute(CCM_COMMAND + " node%d stop", node);
  }

  @Override
  public boolean isMultiDC() {
    return nodesPerDC.length > 1;
  }

  @Override
  public synchronized void startDC(int dc) {
    for (int node = 1; node <= nodesPerDC[dc - 1]; node++) {
      start(dc, node);
    }
  }

  @Override
  public synchronized void stopDC(int dc) {
    for (int node = 1; node <= nodesPerDC[dc - 1]; node++) {
      stop(dc, node);
    }
  }

  @Override
  public String getDC(int node) {
    String status = execute(CCM_COMMAND + " node%d status", node);
    Matcher matcher = DATACENTER_PATTERN.matcher(status);
    if (matcher.find()) {
      return matcher.group(1);
    }
    throw new IllegalStateException("Could not determine DC name for node " + node);
  }

  @Override
  public synchronized void start(int dc, int node) {
    start(NetworkUtils.absoluteNodeNumber(nodesPerDC, dc, node));
  }

  @Override
  public synchronized void stop(int dc, int node) {
    stop(NetworkUtils.absoluteNodeNumber(nodesPerDC, dc, node));
  }

  @Override
  public synchronized void forceStop(int n) {
    LOGGER.debug(
        String.format(
            "Force stopping: node %s (%s%s:%s) in %s", n, DEFAULT_IP_PREFIX, n, binaryPort, this));
    execute(CCM_COMMAND + " node%d stop --not-gently", n);
  }

  @Override
  public synchronized void remove(int n) {
    LOGGER.debug(
        String.format(
            "Removing: node %s (%s%s:%s) from %s", n, DEFAULT_IP_PREFIX, n, binaryPort, this));
    execute(CCM_COMMAND + " node%d remove", n);
  }

  @Override
  public synchronized void add(int n) {
    add(1, n);
  }

  @Override
  public synchronized void add(int dc, int n) {
    LOGGER.debug(
        String.format(
            "Adding: node %s (%s%s:%s) to %s", n, DEFAULT_IP_PREFIX, n, binaryPort, this));
    String ip = addressOfNode(n).getAddress().getHostAddress();
    String thriftItf = ip + ":" + thriftPort;
    String storageItf = ip + ":" + storagePort;
    String binaryItf = ip + ":" + binaryPort;
    String remoteLogItf = ip + ":" + findAvailablePort();
    if (CCM_VERSION.compareTo(V6_0_0) >= 0) {
      execute(
          CCM_COMMAND
              + " add node%d -d dc%s -i %s -l %s --binary-itf %s -j %d -r %s -s -b"
              + (CCM_TYPE == DSE ? " --dse" : (CCM_TYPE == DDAC ? " --ddac" : "")),
          n,
          dc,
          ip,
          storageItf,
          binaryItf,
          findAvailablePort(),
          remoteLogItf);
    } else {
      execute(
          CCM_COMMAND
              + " add node%d -d dc%s -i %s -t %s -l %s --binary-itf %s -j %d -r %s -s -b"
              + (CCM_TYPE == DSE ? " --dse" : (CCM_TYPE == DDAC ? " --ddac" : "")),
          n,
          dc,
          ip,
          thriftItf,
          storageItf,
          binaryItf,
          findAvailablePort(),
          remoteLogItf);
    }
  }

  @Override
  public synchronized void decommission(int n) {
    LOGGER.debug(
        String.format(
            "Decommissioning: node %s (%s:%s) from %s", n, addressOfNode(n), binaryPort, this));
    execute(CCM_COMMAND + " node%d decommission", n);
  }

  @Override
  public synchronized void updateConfig(Map<String, Object> configs) {
    StringBuilder confStr = new StringBuilder();
    for (Map.Entry<String, Object> entry : configs.entrySet()) {
      confStr.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
    }
    execute(CCM_COMMAND + " updateconf " + confStr);
  }

  @Override
  public synchronized void updateDSEConfig(Map<String, Object> configs) {
    StringBuilder confStr = new StringBuilder();
    for (Map.Entry<String, Object> entry : configs.entrySet()) {
      confStr.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
    }
    execute(CCM_COMMAND + " updatedseconf " + confStr);
  }

  @Override
  public synchronized void updateNodeConfig(int n, String key, Object value) {
    updateNodeConfig(n, Collections.singletonMap(key, value));
  }

  @Override
  public synchronized void updateNodeConfig(int n, Map<String, Object> configs) {
    StringBuilder confStr = new StringBuilder();
    for (Map.Entry<String, Object> entry : configs.entrySet()) {
      confStr.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
    }
    execute(CCM_COMMAND + " node%s updateconf %s", n, confStr);
  }

  @Override
  public synchronized void updateDSENodeConfig(int n, String key, Object value) {
    updateDSENodeConfig(n, Collections.singletonMap(key, value));
  }

  @Override
  public synchronized void updateDSENodeConfig(int n, Map<String, Object> configs) {
    StringBuilder confStr = new StringBuilder();
    for (Map.Entry<String, Object> entry : configs.entrySet()) {
      confStr.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
    }
    execute(CCM_COMMAND + " node%s updatedseconf %s", n, confStr);
  }

  @Override
  public synchronized void setWorkload(int node, Workload... workload) {
    String workloadStr = Joiner.on(",").join(workload);
    execute(CCM_COMMAND + " node%d setworkload %s", node, workloadStr);
  }

  private synchronized String execute(String command, Object... args) {
    String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
    // 10 minutes timeout
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    StringWriter sw = new StringWriter();
    StringWriter swOut = new StringWriter();
    StringWriter swErr = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    PrintWriter pwOut = new PrintWriter(swOut);
    PrintWriter pwErr = new PrintWriter(swErr);
    try (Closer closer = Closer.create()) {
      closer.register(pw);
      closer.register(pwOut);
      closer.register(pwErr);
      LOGGER.trace("Executing: " + fullCommand);
      CommandLine cli = CommandLine.parse(fullCommand);
      Executor executor = new DefaultExecutor();
      LogOutputStream outStream =
          new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
              CCM_OUT_LOGGER.debug(line);
              pw.println(line);
              pwOut.println(line);
            }
          };
      LogOutputStream errStream =
          new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
              CCM_ERR_LOGGER.error(line);
              pw.println(line);
              pwErr.println(line);
            }
          };
      closer.register(outStream);
      closer.register(errStream);
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);
      int retValue = executor.execute(cli, ENVIRONMENT_MAP);
      if (retValue != 0) {
        LOGGER.error(
            "Non-zero exit code ({}) returned from executing ccm command: {}",
            retValue,
            fullCommand);
        pwOut.flush();
        pwErr.flush();
        throw new CCMException(
            String.format(
                "Non-zero exit code (%s) returned from executing ccm command: %s",
                retValue, fullCommand),
            fullCommand,
            swOut.toString(),
            swErr.toString());
      }
    } catch (IOException e) {
      if (watchDog.killedProcess()) {
        LOGGER.error("The command {} was killed after 10 minutes", fullCommand);
      }
      pwOut.flush();
      pwErr.flush();
      throw new CCMException(
          String.format("The command %s failed to execute", fullCommand),
          fullCommand,
          swOut.toString(),
          swErr.toString(),
          e);
    } finally {
      pw.flush();
    }
    return sw.toString();
  }

  @Override
  public void waitForUp(int node) {
    NetworkUtils.waitUntilPortIsUp(addressOfNode(node));
  }

  @Override
  public void waitForDown(int node) {
    NetworkUtils.waitUntilPortIsDown(addressOfNode(node));
  }

  @Override
  public void waitForUp(int dc, int node) {
    NetworkUtils.waitUntilPortIsUp(addressOfNode(dc, node));
  }

  @Override
  public void waitForDown(int dc, int node) {
    NetworkUtils.waitUntilPortIsDown(addressOfNode(dc, node));
  }

  @Override
  public String toString() {
    return String.format("CCM cluster %s (%s %s)", clusterName, getClusterType(), getVersion());
  }

  private void handleCCMException(CCMException e) {
    LOGGER.error("CCM output:\n{}", e.getOut());
    setKeepLogs();
    String errors = checkForErrors();
    if (errors != null && !errors.isEmpty()) {
      LOGGER.error("CCM check errors:\n{}", errors);
    }
    throw e;
  }

  enum State {
    CREATED {
      @Override
      boolean canTransitionTo(State state) {
        return state == STARTED || state == REMOVED;
      }
    },
    STARTED {
      @Override
      boolean canTransitionTo(State state) {
        return state == STOPPED;
      }
    },
    STOPPED {
      @Override
      boolean canTransitionTo(State state) {
        return state == STARTED || state == REMOVED;
      }
    },
    REMOVED {
      @Override
      boolean canTransitionTo(State state) {
        return false;
      }
    };

    abstract boolean canTransitionTo(State state);
  }

  /**
   * A builder for {@link DefaultCCMCluster} instances. Use {@link #builder()} to get an instance of
   * this builder.
   */
  @SuppressWarnings("UnusedReturnValue")
  public static class Builder {

    private static final String RANDOM_PORT = "__RANDOM_PORT__";

    private static final Pattern RANDOM_PORT_PATTERN = Pattern.compile(RANDOM_PORT);

    private int[] nodes = {1};
    private final Set<String> createOptions = new LinkedHashSet<>(DEFAULT_CREATE_OPTIONS);
    private final Set<String> jvmArgs = new LinkedHashSet<>();
    private final Map<String, Object> cassandraConfiguration = new LinkedHashMap<>();
    private final Map<String, Object> dseConfiguration = new LinkedHashMap<>();
    private final Map<Integer, Workload[]> workloads = new HashMap<>();

    private Builder() {
      cassandraConfiguration.put("start_rpc", false);
      cassandraConfiguration.put("storage_port", RANDOM_PORT);
      cassandraConfiguration.put("rpc_port", RANDOM_PORT);
      cassandraConfiguration.put("native_transport_port", RANDOM_PORT);
    }

    /** Number of hosts for each DC. Defaults to {@code [1]} (1 DC with 1 node). */
    public Builder withNodes(int... nodes) {
      this.nodes = nodes;
      return this;
    }

    /** Enables SSL encryption. */
    public Builder withSSL() {
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      cassandraConfiguration.put(
          "client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      return this;
    }

    /** Enables client authentication. This also enables encryption ({@link #withSSL()}. */
    public Builder withAuth() {
      withSSL();
      cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
      cassandraConfiguration.put(
          "client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
      return this;
    }

    /** Free-form options that will be added at the end of the {@code ccm create} command. */
    public Builder withCreateOptions(String... createOptions) {
      Collections.addAll(this.createOptions, createOptions);
      return this;
    }

    /** Customizes entries in cassandra.yaml (can be called multiple times) */
    public Builder withCassandraConfiguration(String key, Object value) {
      this.cassandraConfiguration.put(key, value);
      return this;
    }

    /** Customizes entries in dse.yaml (can be called multiple times) */
    public Builder withDSEConfiguration(String key, Object value) {
      this.dseConfiguration.put(key, value);
      return this;
    }

    /**
     * JVM args to use when starting hosts. System properties should be provided one by one, as a
     * string in the form: {@code -Dname=value}.
     */
    public Builder withJvmArgs(String... jvmArgs) {
      Collections.addAll(this.jvmArgs, jvmArgs);
      return this;
    }

    /**
     * Sets the DSE workload for a given node.
     *
     * @param node The node to set the workload for (starting with 1).
     * @param workload The workload(s) (e.g. solr, spark, hadoop)
     * @return This builder
     */
    public Builder withWorkload(int node, Workload... workload) {
      this.workloads.put(node, workload);
      return this;
    }

    public DefaultCCMCluster build() {
      // be careful NOT to alter internal state (hashCode/equals) during build!
      String clusterName = StringUtils.uniqueIdentifier("ccm");
      Map<String, Object> cassandraConfiguration = randomizePorts(this.cassandraConfiguration);
      Map<String, Object> dseConfiguration = randomizePorts(this.dseConfiguration);
      if (CCM_TYPE == DSE && CCM_VERSION.compareTo(V5_0_0) >= 0) {
        if (!dseConfiguration.containsKey("lease_netty_server_port")) {
          dseConfiguration.put("lease_netty_server_port", findAvailablePort());
        }
        if (!dseConfiguration.containsKey("internode_messaging_options.port")) {
          dseConfiguration.put("internode_messaging_options.port", findAvailablePort());
        }
        // only useful if at least one node has graph workload
        if (!dseConfiguration.containsKey("graph.gremlin_server.port")) {
          dseConfiguration.put("graph.gremlin_server.port", findAvailablePort());
        }
      }
      if (((CCM_TYPE == DSE) && (CCM_VERSION.compareTo(V5_0_0) < 0))
          || ((CCM_TYPE == OSS) && (CCM_VERSION.compareTo(V2_2_0) < 0))) {
        cassandraConfiguration.remove("enable_user_defined_functions");
      }
      int storagePort = Integer.parseInt(cassandraConfiguration.get("storage_port").toString());
      int thriftPort = Integer.parseInt(cassandraConfiguration.get("rpc_port").toString());
      int binaryPort =
          Integer.parseInt(cassandraConfiguration.get("native_transport_port").toString());
      if (CCM_TYPE == DSE && CCM_VERSION.compareTo(V6_0_0) >= 0) {
        cassandraConfiguration.remove("start_rpc");
        cassandraConfiguration.remove("rpc_port");
      }
      if (CCM_TYPE == DSE && CCM_VERSION.compareTo(V4_6_0) < 0) {
        dseConfiguration.remove("cql_slow_log_options.enabled");
      }
      DefaultCCMCluster ccm =
          new DefaultCCMCluster(
              clusterName,
              CCM_TYPE,
              CCM_VERSION,
              cassandraVersion(),
              nodes,
              binaryPort,
              thriftPort,
              storagePort,
              joinJvmArgs());
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    ccm.close();
                    ccm.remove();
                  }));
      ccm.execute(buildCreateCommand(clusterName));
      updateNodeConf(ccm);
      ccm.updateConfig(cassandraConfiguration);
      if (CCM_TYPE == DSE) {
        if (!dseConfiguration.isEmpty()) {
          ccm.updateDSEConfig(dseConfiguration);
        }
        for (Map.Entry<Integer, Workload[]> entry : workloads.entrySet()) {
          ccm.setWorkload(entry.getKey(), entry.getValue());
        }
      }
      return ccm;
    }

    private static Version cassandraVersion() {
      if (CCM_TYPE == DSE) {
        if (CCM_VERSION.compareTo(V6_0_0) >= 0) {
          return V4_0_0;
        } else if (CCM_VERSION.compareTo(V5_1_0) >= 0) {
          return V3_10;
        } else if (CCM_VERSION.compareTo(V5_0_0) >= 0) {
          return V3_0_15;
        } else if (CCM_VERSION.compareTo(V4_8_0) >= 0) {
          return V2_1_19;
        } else if (CCM_VERSION.compareTo(V4_7_0) >= 0) {
          return V2_1_11;
        } else {
          return V2_0_14;
        }
      }
      if (CCM_TYPE == DDAC) {
        return V3_11_2;
      }
      return CCM_VERSION;
    }

    private String joinJvmArgs() {
      StringBuilder allJvmArgs = new StringBuilder();
      for (String jvmArg : jvmArgs) {
        allJvmArgs.append(" --jvm_arg=");
        allJvmArgs.append(randomizePorts(jvmArg));
      }
      return allJvmArgs.toString();
    }

    private String buildCreateCommand(String clusterName) {
      StringBuilder result = new StringBuilder(CCM_COMMAND + " create");
      result.append(" ").append(clusterName);
      result.append(" -i ").append(DEFAULT_IP_PREFIX);
      result.append(" ");
      if (nodes.length > 0) {
        result.append("-n ");
        for (int i = 0; i < nodes.length; i++) {
          int node = nodes[i];
          if (i > 0) {
            result.append(':');
          }
          result.append(node);
        }
      }
      result.append(" ").append(Joiner.on(" ").join(randomizePorts(createOptions)));
      return result.toString();
    }

    /**
     * This is a workaround for an oddity in CCM: when we create a cluster with -n option and
     * non-standard ports, the node.conf files are not updated accordingly.
     */
    private void updateNodeConf(DefaultCCMCluster ccm) {
      int n = 1;
      try (Closer closer = Closer.create()) {
        for (int dc = 1; dc <= nodes.length; dc++) {
          int nodesInDc = nodes[dc - 1];
          for (int i = 0; i < nodesInDc; i++) {
            int jmxPort = findAvailablePort();
            int debugPort = findAvailablePort();
            LOGGER.trace(
                "Node {} in cluster {} using JMX port {} and debug port {}",
                n,
                ccm.getClusterName(),
                jmxPort,
                debugPort);
            File nodeConf = new File(ccm.getNodeDir(n), "node.conf");
            File nodeConf2 = new File(ccm.getNodeDir(n), "node.conf.tmp");
            BufferedReader br =
                closer.register(
                    new BufferedReader(
                        new InputStreamReader(
                            new FileInputStream(nodeConf), StandardCharsets.UTF_8.name())));
            PrintWriter pw =
                closer.register(new PrintWriter(nodeConf2, StandardCharsets.UTF_8.name()));
            String line;
            while ((line = br.readLine()) != null) {
              line =
                  line.replace("9042", Integer.toString(ccm.binaryPort))
                      .replace("9160", Integer.toString(ccm.thriftPort))
                      .replace("7000", Integer.toString(ccm.storagePort));
              if (line.startsWith("jmx_port")) {
                line = String.format("jmx_port: '%s'", jmxPort);
              } else if (line.startsWith("remote_debug_port")) {
                String ip = NetworkUtils.addressOfNode(DEFAULT_IP_PREFIX, n).getHostAddress();
                line = String.format("remote_debug_port: %s:%s", ip, debugPort);
              }
              pw.println(line);
            }
            pw.flush();
            pw.close();
            Files.move(nodeConf2, nodeConf);
            n++;
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private Set<String> randomizePorts(Set<String> set) {
      Set<String> randomized = new LinkedHashSet<>();
      for (String value : set) {
        randomized.add(randomizePorts(value));
      }
      return randomized;
    }

    private Map<String, Object> randomizePorts(Map<String, Object> map) {
      Map<String, Object> randomized = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof CharSequence) {
          value = randomizePorts((CharSequence) value);
        }
        randomized.put(entry.getKey(), value);
      }
      return randomized;
    }

    private String randomizePorts(CharSequence str) {
      Matcher matcher = RANDOM_PORT_PATTERN.matcher(str);
      StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        matcher.appendReplacement(sb, Integer.toString(findAvailablePort()));
      }
      matcher.appendTail(sb);
      return sb.toString();
    }

    @Override
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equals(Object o) {
      // do not include cluster name and start, only
      // properties relevant to the settings of the cluster
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Builder builder = (Builder) o;
      if (!Arrays.equals(nodes, builder.nodes)) {
        return false;
      }
      if (!createOptions.equals(builder.createOptions)) {
        return false;
      }
      if (!jvmArgs.equals(builder.jvmArgs)) {
        return false;
      }
      if (!cassandraConfiguration.equals(builder.cassandraConfiguration)) {
        return false;
      }
      if (!dseConfiguration.equals(builder.dseConfiguration)) {
        return false;
      }
      return workloads.equals(builder.workloads);
    }

    @Override
    public int hashCode() {
      // do not include cluster name and start, only
      // properties relevant to the settings of the cluster
      int result = Arrays.hashCode(nodes);
      result = 31 * result + createOptions.hashCode();
      result = 31 * result + jvmArgs.hashCode();
      result = 31 * result + cassandraConfiguration.hashCode();
      result = 31 * result + dseConfiguration.hashCode();
      result = 31 * result + workloads.hashCode();
      return result;
    }
  }
}
