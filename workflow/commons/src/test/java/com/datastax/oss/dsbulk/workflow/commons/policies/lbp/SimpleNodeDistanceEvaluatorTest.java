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
package com.datastax.oss.dsbulk.workflow.commons.policies.lbp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoSession;

@TestInstance(Lifecycle.PER_CLASS)
class SimpleNodeDistanceEvaluatorTest {

  @Mock Node n1;
  @Mock Node n2;

  @Mock EndPoint e1;
  @Mock EndPoint e2;

  private MockitoSession mockito;

  @BeforeAll
  void startMocking() {
    mockito = Mockito.mockitoSession().initMocks(this).startMocking();
    when(n1.getEndPoint()).thenReturn(e1);
    when(n2.getEndPoint()).thenReturn(e2);
  }

  @AfterAll
  void finishMocking() {
    mockito.finishMocking();
  }

  @ParameterizedTest
  @MethodSource
  void should_include_or_exclude_nodes(
      Set<EndPoint> includedHosts,
      Set<EndPoint> excludedHosts,
      NodeDistance expected1,
      NodeDistance expected2) {
    // when
    SimpleNodeDistanceEvaluator filter =
        new SimpleNodeDistanceEvaluator(includedHosts, excludedHosts);
    // then
    assertThat(filter.evaluateDistance(n1, "dc1")).isEqualTo(expected1);
    assertThat(filter.evaluateDistance(n2, "dc1")).isEqualTo(expected2);
  }

  @SuppressWarnings("unused")
  Stream<Arguments> should_include_or_exclude_nodes() {
    return Stream.of(
        Arguments.of(ImmutableSet.of(), ImmutableSet.of(), null, null),
        Arguments.of(ImmutableSet.of(e1), ImmutableSet.of(), null, NodeDistance.IGNORED),
        Arguments.of(ImmutableSet.of(e1, e2), ImmutableSet.of(), null, null),
        Arguments.of(ImmutableSet.of(), ImmutableSet.of(e1), NodeDistance.IGNORED, null),
        Arguments.of(
            ImmutableSet.of(), ImmutableSet.of(e1, e2), NodeDistance.IGNORED, NodeDistance.IGNORED),
        Arguments.of(
            ImmutableSet.of(e1),
            ImmutableSet.of(e1, e2),
            NodeDistance.IGNORED,
            NodeDistance.IGNORED),
        Arguments.of(ImmutableSet.of(e1, e2), ImmutableSet.of(e1), NodeDistance.IGNORED, null),
        Arguments.of(
            ImmutableSet.of(e1, e2),
            ImmutableSet.of(e1, e2),
            NodeDistance.IGNORED,
            NodeDistance.IGNORED));
  }
}
