package com.datastax.dsbulk.engine.internal.utils;

import java.util.List;
import java.util.Set;

public class ClusterInformation {
  private final String partitioner;
  private final int numberOfHosts;
  private final Set<String> dataCenters;
  private final List<String> hostsInfo;
  private boolean someNodesOmitted;

  ClusterInformation(
      String partitioner,
      int numberOfHosts,
      Set<String> dataCenters,
      List<String> hostsInfo,
      boolean someNodesOmitted) {
    this.partitioner = partitioner;
    this.numberOfHosts = numberOfHosts;
    this.dataCenters = dataCenters;
    this.hostsInfo = hostsInfo;
    this.someNodesOmitted = someNodesOmitted;
  }

  String getPartitioner() {
    return partitioner;
  }

  int getNumberOfHosts() {
    return numberOfHosts;
  }

  Set<String> getDataCenters() {
    return dataCenters;
  }

  List<String> getHostsInfo() {
    return hostsInfo;
  }

  public boolean isSomeNodesOmitted() {
    return someNodesOmitted;
  }
}
