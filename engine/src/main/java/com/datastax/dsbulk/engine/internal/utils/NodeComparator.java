/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Comparator;

public class NodeComparator implements Comparator<Node> {

  @Override
  public int compare(Node o1, Node o2) {
    if (o1.equals(o2)) {
      return 0;
    } else if (getHostName(o1) == null || getHostName(o2) == null) {
      return 0;
    } else {
      return getHostName(o1).compareTo(getHostName(o2));
    }
  }

  private String getHostName(Node n) {
    SocketAddress address = n.getEndPoint().resolve();
    if (address instanceof InetSocketAddress) {
      return ((InetSocketAddress) address).getHostName();
    }
    return n.getBroadcastAddress().map(InetSocketAddress::getHostName).orElse(null);
  }
}
