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
package com.datastax.oss.dsbulk.workflow.commons.utils;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.primitives.Ints;
import com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Comparator;

public class NodeComparator implements Comparator<Node> {

  @Override
  public int compare(Node node1, Node node2) {
    if (node1 == node2 || node1.equals(node2)) {
      return 0;
    }
    InetSocketAddress addr1 = getInetSocketAddress(node1);
    InetSocketAddress addr2 = getInetSocketAddress(node2);
    if (addr1 == null || addr2 == null) {
      return 0;
    }
    if (addr1.isUnresolved() || addr2.isUnresolved()) {
      String host1 = addr1.getHostString();
      String host2 = addr2.getHostString();
      int result = host1.compareTo(host2);
      if (result != 0) {
        return result;
      }
    } else {
      byte[] ip1 = addr1.getAddress().getAddress();
      byte[] ip2 = addr2.getAddress().getAddress();
      int result = UnsignedBytes.lexicographicalComparator().compare(ip1, ip2);
      if (result != 0) {
        return result;
      }
    }
    return Ints.compare(addr1.getPort(), addr2.getPort());
  }

  @Nullable
  private InetSocketAddress getInetSocketAddress(Node node) {
    SocketAddress address = node.getEndPoint().resolve();
    if (address instanceof InetSocketAddress) {
      return (InetSocketAddress) address;
    }
    return node.getBroadcastAddress().orElse(null);
  }
}
