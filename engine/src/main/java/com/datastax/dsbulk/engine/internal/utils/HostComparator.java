package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.driver.core.Host;
import java.util.Comparator;

public class HostComparator implements Comparator<Host> {
  @Override
  public int compare(Host o1, Host o2) {
    if (o1 == o2) {
      return 0;
    } else if (o1.getSocketAddress() == null || o2.getSocketAddress() == null) {
      return 0;
    } else if (o1.getSocketAddress().getHostName() == null
        || o2.getSocketAddress().getHostName() == null) {
      return 0;
    } else {
      return o1.getSocketAddress().getHostName().compareTo(o2.getSocketAddress().getHostName());
    }
  }
}
