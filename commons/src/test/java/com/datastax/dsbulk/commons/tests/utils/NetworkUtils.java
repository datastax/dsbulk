/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

  /**
   * The default IP prefix to use when assigning Ip addresses to nodes in a remote session.
   *
   * <p>By default, the prefix is {@code 127.0.0.}, which means that IP addresses will be allocated
   * starting with {@code 127.0.0.1}, {@code 127.0.0.2}, etc.
   *
   * <p>The prefix can be changed with the system property {@code
   * com.datastax.dsbulk.commons.tests.utils.DEFAULT_IP_PREFIX}.
   */
  public static final String DEFAULT_IP_PREFIX =
      System.getProperty("com.datastax.dsbulk.commons.tests.utils.DEFAULT_IP_PREFIX", "127.0.0.");

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkUtils.class);

  /**
   * Returns the address of the {@code n}th host in the cluster (counting from 1, e.g. {@code
   * addressOfNode("127.0.0.", 3)} returns the address of the third node in the cluster, that is,
   * {@code 127.0.0.3}.
   *
   * <p>In multi-DC setups, nodes are numbered in ascending order of their datacenter number. E.g.
   * with 2 DCs and 3 nodes in each DC, the first node in DC 2 is number 4.
   *
   * @param ipPrefix The IP prefix to use (e.g. {@code 127.0.0.}).
   * @param node the node number (starting from 1).
   * @return the address of the host in the cluster.
   */
  public static InetAddress addressOfNode(String ipPrefix, int node) {
    String host = ipPrefix + node;
    try {
      return InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      LOGGER.error("Invalid or unreachable host: " + host, e);
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns the address of the {@code n}th host in the {@code dc}th DC in the cluster (counting
   * from 1, e.g. {@code addressOfNode("127.0.1.", {3,3}, 2, 1)} returns the address of the first
   * node of the second DC; because this cluster has 2 DCs with 3 nodes each, that node would
   * actually be {@code 127.0.1.4}.
   *
   * @param ipPrefix The IP prefix to use (e.g. {@code 127.0.1.}).
   * @param nodesPerDC the number of nodes in each DC.
   * @param dc the DC number (starting from 1).
   * @param node the node number (starting from 1).
   * @return the address of the host in the cluster.
   * @throws IndexOutOfBoundsException if {@code dc} or {@code n} are out of bounds.
   */
  public static InetAddress addressOfNode(String ipPrefix, int[] nodesPerDC, int dc, int node) {
    if (dc < 1 || dc > nodesPerDC.length) {
      throw new IndexOutOfBoundsException("Invalid DC number: " + dc);
    }
    if (node < 1 || node > nodesPerDC[dc - 1]) {
      throw new IndexOutOfBoundsException(
          String.format("Invalid node number: %s for DC %s", node, dc));
    }
    return addressOfNode(ipPrefix, absoluteNodeNumber(nodesPerDC, dc, node));
  }

  /**
   * Converts a node number relative to its DC to an "absolute" number (i.e., relative to the whole
   * cluster).
   *
   * @param nodesPerDC the number of nodes in each DC.
   * @param dc the DC number (starting from 1).
   * @param node the node number (starting from 1).
   * @return the absolute node number (starting from 1).
   */
  public static int absoluteNodeNumber(int[] nodesPerDC, int dc, int node) {
    int n = 0;
    for (int i = 1; i < dc; i++) {
      n += nodesPerDC[i - 1];
    }
    return n + node;
  }

  /**
   * Returns all contact points for a given IP prefix and given numbers of nodes per DC.
   *
   * <p>The returned addresses can be used as contact points for clients wishing to connect to the
   * remote cluster, e.g. when building a {@code Session} instance with the DataStax Java driver.
   *
   * @param ipPrefix The IP prefix to use (e.g. {@code 127.0.1.}).
   * @param nodesPerDC the number of nodes in each DC.
   * @return the contact points for the remote cluster.
   */
  public static List<InetAddress> allContactPoints(String ipPrefix, int[] nodesPerDC) {
    List<InetAddress> contactPoints = new ArrayList<>();
    for (int dc = 1; dc <= nodesPerDC.length; dc++) {
      int nodesInDc = nodesPerDC[dc - 1];
      for (int n = 1; n <= nodesInDc; n++) {
        InetAddress address = addressOfNode(ipPrefix, nodesPerDC, dc, n);
        contactPoints.add(address);
      }
    }
    return contactPoints;
  }

  /**
   * Finds an available port in the ephemeral range. This is loosely inspired by Apache MINA's
   * AvailablePortFinder.
   *
   * @return A local port that is currently unused.
   */
  public static synchronized int findAvailablePort() throws UncheckedIOException {
    // let the system pick an ephemeral port
    try (ServerSocket ss = new ServerSocket(0)) {
      ss.setReuseAddress(true);
      return ss.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void waitUntilPortIsUp(InetSocketAddress address) {
    await().atMost(5, MINUTES).until(() -> pingPort(address));
  }

  public static void waitUntilPortIsDown(InetSocketAddress address) {
    await().atMost(5, MINUTES).until(() -> !pingPort(address));
  }

  private static boolean pingPort(InetSocketAddress address) {
    return pingPort(address.getAddress(), address.getPort());
  }

  private static boolean pingPort(InetAddress address, int port) {
    LOGGER.debug("Pinging {}:{}...", address, port);
    boolean connectionSuccessful = false;
    try (Socket ignored = new Socket(address, port)) {
      connectionSuccessful = true;
      LOGGER.debug("Successfully connected");
    } catch (IOException e) {
      LOGGER.debug("Connection failed", e);
    }
    return connectionSuccessful;
  }
}
