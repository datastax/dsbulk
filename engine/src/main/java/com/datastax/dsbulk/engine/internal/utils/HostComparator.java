/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.driver.core.Host;
import java.util.Comparator;

public class HostComparator implements Comparator<Host> {
  @Override
  public int compare(Host o1, Host o2) {
    if (o1.equals(o2)) {
      return 0;
    } else if (o1.getSocketAddress().getHostName() == null
        || o2.getSocketAddress().getHostName() == null) {
      return 0;
    } else {
      return o1.getSocketAddress().getHostName().compareTo(o2.getSocketAddress().getHostName());
    }
  }
}
