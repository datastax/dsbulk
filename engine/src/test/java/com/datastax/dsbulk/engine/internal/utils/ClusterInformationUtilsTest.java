package com.datastax.dsbulk.engine.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.VersionNumber;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class ClusterInformationUtilsTest {

  @Test
  void should_get_information_about_cluster_with_one_host() throws UnknownHostException {
    // given
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster.getDataCenters()).isEqualTo(Sets.newLinkedHashSet("dc1"));
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfHosts()).isEqualTo(1);
    assertThat(infoAboutCluster.getHostsInfo())
        .isEqualTo(
            Collections.singletonList(
                "address: /1.2.3.4, dseVersion: 6.7.0, cassandraVersion: null, dataCenter: dc1"));
  }

  @Test
  void should_get_information_about_cluster_with_two_hosts() throws UnknownHostException {
    // given
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    Host h2 = createHost("dc1", "1.2.3.5");
    when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1, h2));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster.getDataCenters()).isEqualTo(Sets.newLinkedHashSet("dc1"));
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfHosts()).isEqualTo(2);
    assertThat(infoAboutCluster.getHostsInfo())
        .isEqualTo(
            Arrays.asList(
                "address: /1.2.3.4, dseVersion: 6.7.0, cassandraVersion: null, dataCenter: dc1",
                "address: /1.2.3.5, dseVersion: 6.7.0, cassandraVersion: null, dataCenter: dc1"));
  }

  @Test
  void should_get_information_about_cluster_with_two_different_dc() throws UnknownHostException {
    // given
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Host h1 = createHost("dc1", "1.2.3.4");
    Host h2 = createHost("dc2", "1.2.3.5");
    when(metadata.getAllHosts()).thenReturn(Sets.newLinkedHashSet(h1, h2));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster.getDataCenters()).isEqualTo(Sets.newLinkedHashSet("dc1", "dc2"));
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfHosts()).isEqualTo(2);
    assertThat(infoAboutCluster.getHostsInfo())
        .isEqualTo(
            Arrays.asList(
                "address: /1.2.3.4, dseVersion: 6.7.0, cassandraVersion: null, dataCenter: dc1",
                "address: /1.2.3.5, dseVersion: 6.7.0, cassandraVersion: null, dataCenter: dc2"));
  }

  @NotNull
  private Host createHost(String dataCenter, String address) throws UnknownHostException {
    Host h1 = mock(Host.class);
    when(h1.getDseVersion()).thenReturn(VersionNumber.parse("6.7.0"));
    when(h1.getAddress()).thenReturn(InetAddress.getByName(address));
    when(h1.getDatacenter()).thenReturn(dataCenter);
    return h1;
  }
}
