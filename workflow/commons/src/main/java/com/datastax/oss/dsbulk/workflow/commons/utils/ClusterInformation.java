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
