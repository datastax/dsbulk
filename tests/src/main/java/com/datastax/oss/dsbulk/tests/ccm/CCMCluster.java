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
package com.datastax.oss.dsbulk.tests.ccm;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.dsbulk.tests.utils.Version;
import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * An abstraction around a Cassandra or DSE cluster managed by the <a
 * href="https://github.com/pcmanus/ccm">CCM tool</a>.
 */
@SuppressWarnings("unused")
public interface CCMCluster extends Closeable {

  /** Different types of CCM clusters. */
  enum Type {

    /** OSS Cassandra. */
    OSS("3.11.5", ""),

    /** DataStax Distribution of Apache Cassandra. */
    DDAC("5.1.11", "--ddac"),

    /** DataStax Enterprise. */
    DSE("6.7.7", "--dse");

    private final String defaultVersion;
    private final String ccmCreateOption;

    Type(String defaultVersion, String ccmCreateOption) {
      this.defaultVersion = defaultVersion;
      this.ccmCreateOption = ccmCreateOption;
    }

    public String getDefaultVersion() {
      return defaultVersion;
    }

    public String getCreateOption() {
      return ccmCreateOption;
    }
  }

  /** Different workloads for DSE. */
  enum Workload {
    cassandra,
    solr,
    hadoop,
    spark,
    cfs,
    graph
  }

  /**
   * Returns the name of this cluster.
   *
   * <p>Implementors should choose unique names to help debugging.
   *
   * @return the name of this cluster.
   */
  String getClusterName();

  /** @return the type of this cluster (OSS, DDAC or DSE). */
  Type getClusterType();

  /**
   * Returns the IP prefix used to assign IP addresses to nodes in this cluster.
   *
   * <p>The IP prefix should be in the form "{@code 127.0.0.}", and nodes would be assigned IP
   * addresses in ascending order, i.e. node 1 would be {@code 127.0.0.1} and so on.
   *
   * @return the IP prefix used to assign IP addresses to nodes in this cluster.
   * @see #addressOfNode(int)
   * @see #addressOfNode(int, int)
   */
  String getIpPrefix();

  /**
   * Returns the binary (a.k.a. native transport) port that clients should use to connect to this
   * cluster using the native protocol.
   *
   * <p>This is equivalent to the <a
   * href="http://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__native_transport_port"
   * >native_transport_port</a> property in the Cassandra YAML configuration file.
   *
   * <p>It is required that implementors use the same binary port for all nodes in the cluster.
   *
   * @return the binary port for this remote cluster.
   * @see #addressOfNode(int)
   * @see #addressOfNode(int, int)
   */
  int getBinaryPort();

  /**
   * Returns all the initial contact points for the current cluster.
   *
   * <p>Note that this method should return as many addresses as there are nodes in the cluster
   * <em>initially</em>; in other words, this method is not required to account for modifications
   * made to the cluster <em>after</em> its creation, such as the decommission of one of its nodes.
   *
   * <p>The returned addresses can be used as contact points for clients wishing to connect to the
   * remote cluster, e.g. when building a {@code Cluster} instance with the DataStax Java driver.
   *
   * @return the initial contact points for the current cluster.
   */
  List<EndPoint> getInitialContactPoints();

  /**
   * Returns the address of the {@code node}th host in the cluster (counting from 1, i.e., {@code
   * addressOfNode(1)} returns the address of the first node).
   *
   * <p>In multi-DC setups, nodes are numbered in ascending order of their datacenter number. E.g.
   * with 2 DCs and 3 nodes in each DC, the first node in DC 2 is number 4.
   *
   * <p>The full address is resolved based on the {@link #getIpPrefix() IP prefix} used by the
   * cluster and on its {@link #getBinaryPort() binary port}.
   *
   * @param node the node number (starting from 1).
   * @return the address of the host in the cluster.
   * @see #getIpPrefix()
   * @see #getBinaryPort()
   */
  InetSocketAddress addressOfNode(int node);

  /**
   * Returns the address of the {@code node}th host in the {@code dc}th DC in the cluster (counting
   * from 1, e.g. {@code addressOfNode(2, 1)} returns the address of the first node in the second
   * DC).
   *
   * <p>The full address is resolved based on the {@link #getIpPrefix() IP prefix} used by the
   * cluster and on its {@link #getBinaryPort() binary port}.
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number, relative to its DC (starting from 1).
   * @return the address of the host in the cluster.
   * @see #getIpPrefix()
   * @see #getBinaryPort()
   */
  InetSocketAddress addressOfNode(int dc, int node);

  /** Starts the whole cluster. */
  void start();

  /** Stops the whole cluster. */
  void stop();

  /**
   * Closes the cluster. This is usually a synonym of {@link #stop()} to comply with {@link
   * Closeable} interface.
   */
  @Override
  void close();

  // Methods altering the cluster

  /**
   * Starts the {@code node}th host in the cluster (counting from 1, i.e., the first node is 1 and
   * so on).
   *
   * @param node the node number (starting from 1).
   */
  void start(int node);

  /**
   * Stops the {@code nth} host in the cluster (counting from 1, i.e., the first node is 1 and so
   * on).
   *
   * @param node the node number (starting from 1).
   */
  void stop(int node);

  /** @return whether or not this CCM cluster has multiple data-centers. */
  boolean isMultiDC();

  /**
   * Returns the DC name the node belongs to.
   *
   * @param node the node to inspect.
   * @return the DC name.
   */
  String getDC(int node);

  /**
   * Starts all nodes in the {@code dc}th DC in the cluster (counting from 1, i.e., the first DC is
   * 1 and so on).
   *
   * @param dc the DC number (starting from 1).
   */
  void startDC(int dc);

  /**
   * Stops all nodes in the {@code dc}th DC in the cluster (counting from 1, i.e., the first DC is 1
   * and so on).
   *
   * @param dc the DC number (starting from 1).
   */
  void stopDC(int dc);

  /**
   * Starts the {@code node}th host in the {@code dc}th DC in the cluster (counting from 1, e.g.
   * {@code stop(2, 1)} starts the first node in the second DC).
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number, relative to its DC (starting from 1).
   */
  void start(int dc, int node);

  /**
   * Stops the {@code node}th host in the {@code dc}th DC in the cluster (counting from 1, e.g.
   * {@code stop(2, 1)} stops the first node in the second DC).
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number, relative to its DC (starting from 1).
   */
  void stop(int dc, int node);

  // Methods blocking until nodes are up or down

  /**
   * Waits for the {@code node}th host to be up by checking its {@link #getBinaryPort() binary
   * port}.
   *
   * <p>This method is meant to block until the host is up, or a timeout occurs, whichever happens
   * first.
   *
   * <p>The timeout threshold is left for implementors to decide.
   *
   * @param node the node number (starting from 1).
   */
  void waitForUp(int node);

  /**
   * Waits for the {@code node}th to be down by checking its {@link #getBinaryPort() binary port}.
   *
   * <p>This method is meant to block until the host is down, or a timeout occurs, whichever happens
   * first.
   *
   * <p>The timeout threshold is left for implementors to decide.
   *
   * @param node the node number (starting from 1).
   */
  void waitForDown(int node);

  /**
   * Waits for the {@code node}th host in the {@code dc}th DC to be up by checking its {@link
   * #getBinaryPort() binary port}.
   *
   * <p>This method is meant to block until the host is up, or a timeout occurs, whichever happens
   * first.
   *
   * <p>The timeout threshold is left for implementors to decide.
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number, relative to its DC (starting from 1).
   */
  void waitForUp(int dc, int node);

  /**
   * Waits for the {@code node}th in the {@code dc}th DC to be down by checking its {@link
   * #getBinaryPort() binary port}.
   *
   * <p>This method is meant to block until the host is down, or a timeout occurs, whichever happens
   * first.
   *
   * <p>The timeout threshold is left for implementors to decide.
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number, relative to its DC (starting from 1).
   */
  void waitForDown(int dc, int node);

  /**
   * Returns the Cassandra or DSE version of this CCM cluster.
   *
   * @return The version of this CCM cluster.
   */
  Version getVersion();

  /**
   * Returns the Cassandra version of this CCM cluster, even if it is a DSE cluster.
   *
   * <p>In case of a DSE cluster, returns the associated Cassandra version; for OSS Cassandra and
   * DDAC, returns the same version as {@link #getVersion()}.
   *
   * @return The Cassandra version of this CCM cluster.
   */
  Version getCassandraVersion();

  /**
   * Returns the factory directory for this CCM cluster.
   *
   * @return The factory directory for this CCM cluster.
   */
  File getCcmDir();

  /**
   * Returns the cluster directory for this CCM cluster.
   *
   * @return The cluster directory for this CCM cluster.
   */
  File getClusterDir();

  /**
   * Returns the node directory for the {@code node}th host in this CCM cluster.
   *
   * @param node the node number, starting with 1.
   * @return The node directory for the {@code node}th host in this CCM cluster.
   */
  File getNodeDir(int node);

  /**
   * Returns the node factory directory for the {@code node}th host in this CCM cluster.
   *
   * @param node the node number, starting with 1.
   * @return The node factory directory for the {@code node}th host in this CCM cluster.
   */
  File getNodeConfDir(int node);

  /**
   * Returns the storage port for this CCM cluster. This is equivalent to the <a
   * href="http://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__storage_port"
   * >storage_port</a> property in the Cassandra YAML configuration file.
   *
   * @return The storage port for this CCM cluster.
   */
  int getStoragePort();

  /**
   * Returns the thrift port (a.k.a. RPC port) for this CCM cluster. This is equivalent to the <a
   * href="http://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__rpc_port"
   * >rpc_port</a> property in the Cassandra YAML configuration file.
   *
   * @return The thrift port for this CCM cluster.
   */
  int getThriftPort();

  /** Signals that logs for this CCM cluster should be kept after the cluster is stopped. */
  void setKeepLogs();

  // Methods altering the whole cluster

  /** Aggressively stops the cluster. */
  void forceStop();

  /** Removes this CCM cluster and deletes all of its files. */
  void remove();

  /**
   * Updates the factory files for all nodes in the CCM cluster.
   *
   * <p>Valid options are those in the Cassandra YAML configuration file.
   *
   * @param configs The configuration options to update.
   */
  void updateConfig(Map<String, Object> configs);

  /**
   * Updates the DSE factory files for all nodes in the CCM cluster.
   *
   * <p>Valid options are those in the DSE YAML configuration file.
   *
   * @param configs the DSE configuration options to update.
   */
  void updateDSEConfig(Map<String, Object> configs);

  /**
   * Checks for errors in the logs of all nodes in the cluster and returns them in a single String.
   */
  String checkForErrors();

  // Methods altering nodes

  /**
   * Aggressively stops the {@code node}th host in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   */
  void forceStop(int node);

  /**
   * Removes the {@code node}th host in the CCM cluster and deletes all of its files.
   *
   * <p>Warning: calling this method might cause methods that manipulate or inspect node numbers to
   * report inaccurate numbers.
   *
   * @param node the node number (starting from 1).
   */
  void remove(int node);

  /**
   * Adds a {@code node}th host in the CCM cluster.
   *
   * <p>Warning: calling this method might cause methods that manipulate or inspect node numbers to
   * report inaccurate numbers.
   *
   * @param node the node number (starting from 1).
   */
  void add(int node);

  /**
   * Adds the {@code node}th host in the CCM cluster.
   *
   * <p>Warning: calling this method might cause methods that manipulate or inspect node numbers to
   * report inaccurate numbers.
   *
   * @param dc the DC number (starting from 1).
   * @param node the node number (starting from 1).
   */
  void add(int dc, int node);

  /**
   * Decommissions the {@code node}th host in the CCM cluster.
   *
   * <p>Warning: calling this method might cause methods that manipulate or inspect node numbers to
   * report inaccurate numbers.
   *
   * @param node the node number (starting from 1).
   */
  void decommission(int node);

  /**
   * Updates the {@code node}th host's factory file in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   * @param key the configuration option name.
   * @param value the configuration option value.
   */
  void updateNodeConfig(int node, String key, Object value);

  /**
   * Updates the {@code node}th host's factory file in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   * @param configs the configuration options to update.
   */
  void updateNodeConfig(int node, Map<String, Object> configs);

  /**
   * Updates the {@code node}th host's DSE factory file in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   * @param key the DSE configuration option name.
   * @param value the DSE configuration option value.
   */
  void updateDSENodeConfig(int node, String key, Object value);

  /**
   * Updates the {@code node}th host's dse factory file in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   * @param configs the DSE configuration options to update.
   */
  void updateDSENodeConfig(int node, Map<String, Object> configs);

  /**
   * Sets the workload(s) for the {@code node}th host in the CCM cluster.
   *
   * @param node the node number (starting from 1).
   */
  void setWorkload(int node, Workload... workload);
}
