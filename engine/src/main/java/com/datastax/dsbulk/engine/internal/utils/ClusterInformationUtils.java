/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterInformationUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInformationUtils.class);
  static final int LIMIT_NODES_INFORMATION = 100;

  public static void printDebugInfoAboutCluster(Cluster cluster) {
    if (LOGGER.isDebugEnabled()) {
      ClusterInformation infoAboutCluster = getInfoAboutCluster(cluster);

      LOGGER.debug("Partitioner: {}", infoAboutCluster.getPartitioner());
      LOGGER.debug("Total number of hosts: {}", infoAboutCluster.getNumberOfHosts());
      LOGGER.debug("DataCenters: {}", infoAboutCluster.getDataCenters());

      LOGGER.debug("Hosts:");
      for (String hostSummary : infoAboutCluster.getHostsInfo()) {
        LOGGER.debug(hostSummary);
      }
      if (infoAboutCluster.isSomeNodesOmitted()) {
        LOGGER.debug("(Other nodes omitted)");
      }
    }
  }

  static ClusterInformation getInfoAboutCluster(Cluster cluster) {

    Metadata metadata = cluster.getMetadata();
    Set<Host> allHosts = metadata.getAllHosts();

    Set<String> dataCenters = getAllDataCenters(allHosts);

    List<String> hostsInfo =
        allHosts
            .stream()
            .sorted(Comparator.comparing(o -> o.getSocketAddress().getHostName()))
            .limit(LIMIT_NODES_INFORMATION)
            .map(ClusterInformationUtils::getHostInfo)
            .collect(Collectors.toCollection(LinkedList::new));
    return new ClusterInformation(
        cluster.getMetadata().getPartitioner(),
        allHosts.size(),
        dataCenters,
        hostsInfo,
        numberOfNodesAboveLimit(allHosts));
  }

  private static boolean numberOfNodesAboveLimit(Set<Host> allHosts) {
    return allHosts.size() > LIMIT_NODES_INFORMATION;
  }

  private static Set<String> getAllDataCenters(Set<Host> allHosts) {
    return allHosts
        .stream()
        .collect(Collectors.groupingBy(Host::getDatacenter))
        .keySet()
        .stream()
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static String getHostInfo(Host h) {
    return String.format(
        "address: %s, dseVersion: %s, cassandraVersion: %s, dataCenter: %s",
        h.getSocketAddress(), h.getDseVersion(), h.getCassandraVersion(), h.getDatacenter());
  }
}
