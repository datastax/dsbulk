/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.utils;

import java.util.List;
import java.util.Set;

class ClusterInformation {
  private final String partitioner;
  private final int numberOfHosts;
  private final Set<String> dataCenters;
  private final List<String> hostsInfo;
  private final boolean someNodesOmitted;

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

  int getNumberOfNodes() {
    return numberOfHosts;
  }

  Set<String> getDataCenters() {
    return dataCenters;
  }

  List<String> getNodeInfos() {
    return hostsInfo;
  }

  public boolean isSomeNodesOmitted() {
    return someNodesOmitted;
  }
}
