package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.LIMIT_NODES_INFORMATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.DEBUG;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.VersionNumber;
import com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
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

  @Test
  void should_limit_information_about_hosts_to_100() throws UnknownHostException {
    // given
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Set<Host> hosts =
        IntStream.range(0, 110)
            .mapToObj(i -> createHost("dc1", "1.2.3." + i))
            .collect(Collectors.toSet());
    when(metadata.getAllHosts()).thenReturn(hosts);

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(cluster);

    // then
    assertThat(infoAboutCluster.getDataCenters()).isEqualTo(Sets.newLinkedHashSet("dc1"));
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfHosts()).isEqualTo(110);
    assertThat(infoAboutCluster.getHostsInfo().size()).isEqualTo(LIMIT_NODES_INFORMATION);
  }

  @Test
  void should_log_cluster_information_in_debug_mode(
      @LogCapture(value = ClusterInformationUtils.class, level = DEBUG)
          LogInterceptor interceptor) {
    // given
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getPartitioner()).thenReturn("simple-partitioner");
    Set<Host> hosts =
        IntStream.range(0, 110)
            .mapToObj(i -> createHost("dc1", "1.2.3." + i))
            .collect(Collectors.toSet());
    when(metadata.getAllHosts()).thenReturn(hosts);

    // when
    ClusterInformationUtils.printDebugInfoAboutCluster(cluster);

    // then
    CommonsAssertions.assertThat(interceptor)
        .hasMessageMatching("Partitioner: simple-partitioner, numberOfHosts: 110");
    CommonsAssertions.assertThat(interceptor).hasMessageMatching("Hosts:");
    CommonsAssertions.assertThat(interceptor).hasMessageMatching("other nodes omitted");
  }

  @NotNull
  private Host createHost(String dataCenter, String address) {
    Host h1 = mock(Host.class);
    when(h1.getDseVersion()).thenReturn(VersionNumber.parse("6.7.0"));
    try {
      when(h1.getAddress()).thenReturn(InetAddress.getByName(address));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    when(h1.getDatacenter()).thenReturn(dataCenter);
    return h1;
  }
}
