/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockNode;
import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.LIMIT_NODES_INFORMATION;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.DEBUG;

import com.datastax.dsbulk.commons.tests.driver.DriverUtils;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class ClusterInformationUtilsTest {

  private static final UUID HOST_ID_1 = UUID.randomUUID();
  private static final UUID HOST_ID_2 = UUID.randomUUID();

  @Test
  void should_get_information_about_cluster_with_one_host() {
    // given
    CqlSession session = DriverUtils.mockSession();
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();
    when(tokenMap.getPartitionerName()).thenReturn("simple-partitioner");
    Node h1 = mockNode(HOST_ID_1, "1.2.3.4", "dc1");
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(HOST_ID_1, h1));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(session);

    // then
    assertThat(infoAboutCluster.getDataCenters()).containsOnly("dc1");
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfNodes()).isEqualTo(1);
    assertThat(infoAboutCluster.getNodeInfos())
        .isEqualTo(
            Collections.singletonList(
                "address: 1.2.3.4:9042, dseVersion: 6.7.0, cassandraVersion: 3.11.1, dataCenter: dc1"));
  }

  @Test
  void should_get_information_about_cluster_with_two_hosts() {
    // given
    CqlSession session = DriverUtils.mockSession();
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();
    when(tokenMap.getPartitionerName()).thenReturn("simple-partitioner");
    Node h1 = mockNode(HOST_ID_1, "1.2.3.4", "dc1");
    Node h2 = mockNode(HOST_ID_2, "1.2.3.5", "dc1");
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(HOST_ID_1, h1, HOST_ID_2, h2));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(session);

    // then
    assertThat(infoAboutCluster.getDataCenters()).containsOnly("dc1");
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfNodes()).isEqualTo(2);
    assertThat(infoAboutCluster.getNodeInfos())
        .isEqualTo(
            Arrays.asList(
                "address: 1.2.3.4:9042, dseVersion: 6.7.0, cassandraVersion: 3.11.1, dataCenter: dc1",
                "address: 1.2.3.5:9042, dseVersion: 6.7.0, cassandraVersion: 3.11.1, dataCenter: dc1"));
  }

  @Test
  void should_get_information_about_cluster_with_two_different_dc() {
    // given
    CqlSession session = DriverUtils.mockSession();
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();
    when(tokenMap.getPartitionerName()).thenReturn("simple-partitioner");
    Node h1 = mockNode(HOST_ID_1, "1.2.3.4", "dc1");
    Node h2 = mockNode(HOST_ID_2, "1.2.3.5", "dc2");
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(HOST_ID_1, h1, HOST_ID_2, h2));

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(session);

    // then
    assertThat(infoAboutCluster.getDataCenters()).containsExactlyInAnyOrder("dc1", "dc2");
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfNodes()).isEqualTo(2);
    assertThat(infoAboutCluster.getNodeInfos())
        .isEqualTo(
            Arrays.asList(
                "address: 1.2.3.4:9042, dseVersion: 6.7.0, cassandraVersion: 3.11.1, dataCenter: dc1",
                "address: 1.2.3.5:9042, dseVersion: 6.7.0, cassandraVersion: 3.11.1, dataCenter: dc2"));
  }

  @Test
  void should_limit_information_about_hosts_to_100() {
    // given
    CqlSession session = DriverUtils.mockSession();
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();
    when(tokenMap.getPartitionerName()).thenReturn("simple-partitioner");
    Map<UUID, Node> nodes =
        IntStream.range(0, 110)
            .mapToObj(i -> mockNode(UUID.randomUUID(), "1.2.3." + i, "dc1"))
            .collect(Collectors.toMap(Node::getHostId, n -> n));
    when(metadata.getNodes()).thenReturn(nodes);

    // when
    ClusterInformation infoAboutCluster = ClusterInformationUtils.getInfoAboutCluster(session);

    // then
    assertThat(infoAboutCluster.getDataCenters()).containsOnly("dc1");
    assertThat(infoAboutCluster.getPartitioner()).isEqualTo("simple-partitioner");
    assertThat(infoAboutCluster.getNumberOfNodes()).isEqualTo(110);
    assertThat(infoAboutCluster.getNodeInfos().size()).isEqualTo(LIMIT_NODES_INFORMATION);
  }

  @Test
  void should_log_cluster_information_in_debug_mode(
      @LogCapture(value = ClusterInformationUtils.class, level = DEBUG)
          LogInterceptor interceptor) {
    // given
    CqlSession session = DriverUtils.mockSession();
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();
    when(tokenMap.getPartitionerName()).thenReturn("simple-partitioner");
    Map<UUID, Node> nodes =
        IntStream.range(0, 110)
            .mapToObj(i -> mockNode(UUID.randomUUID(), "1.2.3." + i, "dc1"))
            .collect(Collectors.toMap(Node::getHostId, n -> n));
    when(metadata.getNodes()).thenReturn(nodes);

    // when
    ClusterInformationUtils.printDebugInfoAboutCluster(session);

    // then
    assertThat(interceptor).hasMessageContaining("Partitioner: simple-partitioner");
    assertThat(interceptor).hasMessageContaining("Total number of nodes: 110");
    assertThat(interceptor).hasMessageContaining("Nodes:");
    assertThat(interceptor).hasMessageContaining("(Other nodes omitted)");
  }
}
