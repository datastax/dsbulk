package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
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

  public static void printDebugInfoAboutCluster(Cluster cluster) {
    if (LOGGER.isDebugEnabled()) {
      ClusterInformation infoAboutCluster = getInfoAboutCluster(cluster);

      LOGGER.debug(
          "Partitioner: {}, numberOfHosts: {}",
          infoAboutCluster.getPartitioner(),
          infoAboutCluster.getNumberOfHosts());
      LOGGER.debug("DataCenters: {}", infoAboutCluster.getDataCenters());
      LOGGER.debug("Hosts:");
      for (String hostSummary : infoAboutCluster.getHostsInfo()) {
        LOGGER.debug(hostSummary);
      }
      if (infoAboutCluster.isSomeNodesOmitted()) {
        LOGGER.debug("other nodes omitted");
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
            .sorted(Comparator.comparing(o -> o.getAddress().getHostAddress()))
            .limit(LIMIT_NODES_INFORMATION)
            .map(ClusterInformationUtils::getHostInfo)
            .collect(Collectors.toList());
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
        h.getAddress(), h.getDseVersion(), h.getCassandraVersion(), h.getDatacenter());
  }
}
