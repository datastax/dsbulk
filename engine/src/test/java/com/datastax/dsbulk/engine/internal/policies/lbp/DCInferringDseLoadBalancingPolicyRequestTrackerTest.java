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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
class DCInferringDseLoadBalancingPolicyRequestTrackerTest
    extends DCInferringDseLoadBalancingPolicyTestBase {

  private DCInferringDseLoadBalancingPolicy policy;
  private long nextNanoTime;

  @BeforeEach
  @Override
  void setUp() {
    super.setUp();
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    policy =
        new DCInferringDseLoadBalancingPolicy(context, DEFAULT_NAME) {
          @Override
          long nanoTime() {
            return nextNanoTime;
          }
        };
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3),
        distanceReporter);
  }

  @Test
  void should_record_first_response_time_on_node_success() {
    // Given
    nextNanoTime = 123;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(node1, value -> assertThat(value.get(0)).isEqualTo(123L))
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  void should_record_second_response_time_on_node_success() {
    // Given
    should_record_first_response_time_on_node_success();
    nextNanoTime = 456;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.get(0)).isEqualTo(123);
              assertThat(value.get(1)).isEqualTo(456);
            })
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  void should_record_further_response_times_on_node_success() {
    // Given
    should_record_second_response_time_on_node_success();
    nextNanoTime = 789;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);
    policy.onNodeSuccess(request, 0, profile, node2, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.get(0)).isEqualTo(456);
              assertThat(value.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.get(0)).isEqualTo(789))
        .doesNotContainKey(node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  void should_record_first_response_time_on_node_error() {
    // Given
    nextNanoTime = 123;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(node1, value -> assertThat(value.get(0)).isEqualTo(123L))
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  void should_record_second_response_time_on_node_error() {
    // Given
    should_record_first_response_time_on_node_error();
    nextNanoTime = 456;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.get(0)).isEqualTo(123);
              assertThat(value.get(1)).isEqualTo(456);
            })
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  void should_record_further_response_times_on_node_error() {
    // Given
    should_record_second_response_time_on_node_error();
    nextNanoTime = 789;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);
    policy.onNodeError(request, iae, 0, profile, node2, logPrefix);

    // Then
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.get(0)).isEqualTo(456);
              assertThat(value.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.get(0)).isEqualTo(789))
        .doesNotContainKey(node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }
}
