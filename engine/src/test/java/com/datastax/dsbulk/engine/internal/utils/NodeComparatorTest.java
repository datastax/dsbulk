/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static java.net.InetSocketAddress.createUnresolved;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NodeComparatorTest {

  @ParameterizedTest(name = "Compare on node1: {0} and node2: {1} should return: {2}")
  @MethodSource
  void should_compare_two_hosts(Node node1, Node node2, int expectedResult) {
    // when
    int compare = new NodeComparator().compare(node1, node2);

    // then
    assertThat(compare).isEqualTo(expectedResult);
  }

  @SuppressWarnings("unused")
  private static Stream<? extends Arguments> should_compare_two_hosts()
      throws UnknownHostException {
    Node n1 = mockNode(createUnresolved("1.2.3.4", 9042));
    return Stream.of(
        Arguments.of(n1, n1, 0),
        Arguments.of(
            mockNode(null), //
            mockNode(null),
            0),
        Arguments.of(
            mockNode(null), //
            mockNode(createUnresolved("1.2.3.4", 9042)),
            0),
        Arguments.of(
            mockNode(createUnresolved("1.2.3.4", 9042)), //
            mockNode(null),
            0),
        // unresolved addresses -> comparison by host string and port
        Arguments.of(
            mockNode(createUnresolved("1.2.3.4", 9042)),
            mockNode(createUnresolved("1.2.3.5", 9042)),
            -1),
        Arguments.of(
            mockNode(createUnresolved("com.datastax.dsbulk1", 9042)),
            mockNode(createUnresolved("com.datastax.dsbulk2", 9042)),
            -1),
        Arguments.of(
            mockNode(createUnresolved("1.2.3.4", 9042)),
            mockNode(createUnresolved("1.2.3.4", 9043)),
            -1),
        // resolved addresses -> comparison by IPs and port
        Arguments.of(
            mockNode(createResolved("1.2.3.4", 9042, new byte[] {1, 2, 3, 4})),
            mockNode(createResolved("1.2.3.5", 9042, new byte[] {1, 2, 3, 5})),
            -1),
        Arguments.of(
            mockNode(createResolved("1.2.3.4", 9042, new byte[] {1, 2, 3, 4})),
            mockNode(createResolved("1.2.3.4", 9043, new byte[] {1, 2, 3, 4})),
            -1));
  }

  private static InetSocketAddress createResolved(String host, int port, byte[] bytes)
      throws UnknownHostException {
    InetAddress ip = InetAddress.getByAddress(host, bytes);
    return new InetSocketAddress(ip, port);
  }

  private static Node mockNode(@Nullable InetSocketAddress address) {
    Node node = mock(Node.class, address == null ? "null" : address.toString());
    if (address == null) {
      EndPoint endpoint = mock(EndPoint.class);
      SocketAddress socket = mock(SocketAddress.class);
      when(endpoint.resolve()).thenReturn(socket);
      when(node.getEndPoint()).thenReturn(endpoint);
      when(node.getBroadcastRpcAddress()).thenReturn(Optional.empty());
    } else {
      when(node.getEndPoint()).thenReturn(new DefaultEndPoint(address));
    }
    return node;
  }
}
