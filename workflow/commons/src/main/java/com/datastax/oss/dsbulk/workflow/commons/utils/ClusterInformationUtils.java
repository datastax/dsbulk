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

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterInformationUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInformationUtils.class);

  static final int LIMIT_NODES_INFORMATION = 100;

  private static final Comparator<Node> NODE_COMPARATOR = new NodeComparator();

  public static void printDebugInfoAboutCluster(CqlSession session) {
    if (LOGGER.isDebugEnabled()) {
      ClusterInformation clusterInfor = getInfoAboutCluster(session);
      LOGGER.debug("Partitioner: {}", clusterInfor.getPartitioner());
      LOGGER.debug("Total number of nodes: {}", clusterInfor.getNumberOfNodes());
      LOGGER.debug("DataCenters: {}", clusterInfor.getDataCenters());
      LOGGER.debug("Nodes:");
      for (String nodeSummary : clusterInfor.getNodeInfos()) {
        LOGGER.debug(nodeSummary);
      }
      if (clusterInfor.isSomeNodesOmitted()) {
        LOGGER.debug("(Other nodes omitted)");
      }
    }
  }

  static ClusterInformation getInfoAboutCluster(CqlSession session) {
    Metadata metadata = session.getMetadata();
    Collection<Node> allNodes = metadata.getNodes().values();
    Set<String> dataCenters = getAllDataCenters(allNodes);
    List<String> hostsInfo =
        allNodes.stream()
            .sorted(NODE_COMPARATOR)
            .limit(LIMIT_NODES_INFORMATION)
            .map(ClusterInformationUtils::getNodeInfo)
            .collect(Collectors.toCollection(ArrayList::new));
    return new ClusterInformation(
        session.getMetadata().getTokenMap().map(TokenMap::getPartitionerName).orElse("?"),
        allNodes.size(),
        dataCenters,
        hostsInfo,
        numberOfNodesAboveLimit(allNodes));
  }

  private static boolean numberOfNodesAboveLimit(Collection<Node> allNodes) {
    return allNodes.size() > LIMIT_NODES_INFORMATION;
  }

  private static Set<String> getAllDataCenters(Collection<Node> allNodes) {
    return allNodes.stream()
        .collect(Collectors.groupingBy(Node::getDatacenter))
        .keySet()
        .stream()
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static String getNodeInfo(Node h) {
    return String.format(
        "address: %s, dseVersion: %s, cassandraVersion: %s, dataCenter: %s",
        h.getEndPoint().resolve(),
        h.getExtras().get(DseNodeProperties.DSE_VERSION),
        h.getCassandraVersion(),
        h.getDatacenter());
  }
}
