package com.datastax.dsbulk.engine.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.VersionNumber;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ClusterInformationUtilsTest {

  @Test
  void should_get_information_about_cluster_with_one_host() throws UnknownHostException {
    // given
    Cluster cluster = Mockito.mock(Cluster.class);
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    Mockito.when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1));

    // when
    String infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster)
        .contains(
            "simple-partitioner",
            "numberOfHosts: 1",
            "address: /1.2.3.4",
            "dseVersion: 6.7.0",
            "dataCenter: dc1");
  }

  @Test
  void should_get_information_about_cluster_with_two_hosts() throws UnknownHostException {
    // given
    Cluster cluster = Mockito.mock(Cluster.class);
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    Host h2 = createHost("dc1", "1.2.3.5");
    Mockito.when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1, h2));

    // when
    String infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster)
        .contains(
            "simple-partitioner",
            "numberOfHosts: 2",
            "address: /1.2.3.4",
            "dseVersion: 6.7.0",
            "dataCenter: dc1",
            "address: /1.2.3.5",
            "DataCenters: [dc1]");
  }

  @Test
  void should_get_information_about_cluster_with_two_different_dc() throws UnknownHostException {
    // given
    Cluster cluster = Mockito.mock(Cluster.class);
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    Host h2 = createHost("dc2", "1.2.3.5");
    Mockito.when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1, h2));

    // when
    String infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster).contains("DataCenters: [dc1, dc2]");
  }

  @NotNull
  private Host createHost(String dataCenter, String address) throws UnknownHostException {
    Host h1 = Mockito.mock(Host.class);
    Mockito.when(h1.getDseVersion()).thenReturn(VersionNumber.parse("6.7.0"));
    Mockito.when(h1.getAddress()).thenReturn(InetAddress.getByName(address));
    Mockito.when(h1.getDatacenter()).thenReturn(dataCenter);
    return h1;
  }
}
