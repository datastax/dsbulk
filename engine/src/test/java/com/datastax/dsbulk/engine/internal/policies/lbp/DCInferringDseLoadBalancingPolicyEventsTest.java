/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.policies.lbp;

import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static com.datastax.oss.driver.api.core.loadbalancing.NodeDistance.IGNORED;
import static com.datastax.oss.driver.api.core.loadbalancing.NodeDistance.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
class DCInferringDseLoadBalancingPolicyEventsTest
    extends DCInferringDseLoadBalancingPolicyTestBase {

  @Test
  void should_remove_down_node_from_live_set() {
    // Given
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onDown(node2);

    // Then
    then(distanceReporter).should(never()).setDistance(eq(node2), any(NodeDistance.class));
    assertThat(policy.localDcLiveNodes).containsOnly(node1);
  }

  @Test
  void should_remove_removed_node_from_live_set() {
    // Given
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onRemove(node2);

    // Then
    then(distanceReporter).should(never()).setDistance(eq(node2), any(NodeDistance.class));
    assertThat(policy.localDcLiveNodes).containsOnly(node1);
  }

  @Test
  void should_set_added_node_to_local() {
    // Given
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onAdd(node3);

    // Then
    // Not added to the live set yet, we're waiting for the pool to open
    then(distanceReporter).should().setDistance(node3, LOCAL);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
  }

  @Test
  void should_ignore_added_node_when_filtered() {
    // Given
    given(filter.test(node3)).willReturn(false);
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onAdd(node3);

    // Then
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
  }

  @Test
  void should_ignore_added_node_when_remote_dc() {
    // Given
    given(node3.getDatacenter()).willReturn("dc2");
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onAdd(node3);

    // Then
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
  }

  @Test
  void should_add_up_node_to_live_set() {
    // Given
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onUp(node3);

    // Then
    then(distanceReporter).should().setDistance(node3, LOCAL);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2, node3);
  }

  @Test
  void should_ignore_up_node_when_filtered() {
    // Given
    given(filter.test(node3)).willReturn(false);
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onUp(node3);

    // Then
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
  }

  @Test
  void should_ignore_up_node_when_remote_dc() {
    // Given
    given(node3.getDatacenter()).willReturn("dc2");
    DCInferringDseLoadBalancingPolicy policy = createAndInitPolicy();

    // When
    policy.onUp(node3);

    // Then
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
  }

  private DCInferringDseLoadBalancingPolicy createAndInitPolicy() {
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    DCInferringDseLoadBalancingPolicy policy =
        new DCInferringDseLoadBalancingPolicy(context, DEFAULT_NAME);
    policy.init(
        ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2), distanceReporter);
    assertThat(policy.localDcLiveNodes).containsOnly(node1, node2);
    reset(distanceReporter);
    return policy;
  }
}
