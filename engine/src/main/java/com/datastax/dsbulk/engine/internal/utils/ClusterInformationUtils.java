package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterInformationUtils {

  public static String getInfoAboutCluster(Cluster cluster) {

    Metadata metadata = cluster.getMetadata();
    Set<Host> allHosts = metadata.getAllHosts();
    String clusterInfo =
        String.format(
            "Partitioner: %s, numberOfHosts: %s",
            cluster.getMetadata().getPartitioner(), allHosts.size());
    Set<String> dataCenters = getAllDcs(allHosts);

    String hostsInfo =
        allHosts
            .stream()
            .map(ClusterInformationUtils::getHostInfo)
            .collect(Collectors.joining("\n"));

    return String.format(
        "Cluster: %s\nDataCenters: %s\nHosts: \n%s", clusterInfo, dataCenters, hostsInfo);
  }

  private static Set<String> getAllDcs(Set<Host> allHosts) {
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
        "address: %s, dseVersion: %s, cassandraVersion: %s, dataCenter: %s, tokens: %s",
        h.getAddress(),
        h.getDseVersion(),
        h.getCassandraVersion(),
        h.getDatacenter(),
        h.getTokens());
  }
}
