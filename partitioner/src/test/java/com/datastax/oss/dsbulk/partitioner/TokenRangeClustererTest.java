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
package com.datastax.oss.dsbulk.partitioner;

import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newToken;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenRange;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

class TokenRangeClustererTest {

  private final Murmur3BulkTokenFactory tokenFactory = new Murmur3BulkTokenFactory();

  private final EndPoint node1 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.1", 9042));
  private final EndPoint node2 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.2", 9042));
  private final EndPoint node3 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.3", 9042));
  private final EndPoint node4 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.4", 9042));

  @Test
  void testEmpty() {
    // given
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    // when
    List<BulkTokenRange> groups = trc.group(Collections.emptyList(), 10, Integer.MAX_VALUE);
    // then
    assertThat(groups.size()).isZero();
  }

  @Test
  void testTrivialClustering() {
    // given
    Murmur3BulkTokenRange tr1 = range(0, 10, set(node1));
    Murmur3BulkTokenRange tr2 = range(10, 20, set(node1));
    // when
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    List<BulkTokenRange> groups = trc.group(list(tr1, tr2), 1, Integer.MAX_VALUE);
    // then
    assertThat(groups.size()).isOne();
    assertThat(groups.get(0)).startsWith(tr1.getStart()).endsWith(tr2.getEnd());
  }

  @Test
  void testSplitByHost() {
    // given
    Murmur3BulkTokenRange tr1 = range(0, 10, set(node1));
    Murmur3BulkTokenRange tr2 = range(10, 20, set(node1));
    Murmur3BulkTokenRange tr3 = range(20, 30, set(node2));
    Murmur3BulkTokenRange tr4 = range(30, 40, set(node2));
    // when
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    List<BulkTokenRange> groups = trc.group(list(tr1, tr2, tr3, tr4), 1, Integer.MAX_VALUE);
    // then
    assertThat(groups.size()).isEqualTo(2);
    assertThat(groups).containsExactly(range(0, 20, set(node1)), range(20, 40, set(node2)));
  }

  @Test
  void testSplitByCount() {
    // given
    Murmur3BulkTokenRange tr1 = range(Long.MIN_VALUE, Long.MIN_VALUE / 2, set(node1));
    Murmur3BulkTokenRange tr2 = range(Long.MIN_VALUE / 2, 0, set(node1));
    Murmur3BulkTokenRange tr3 = range(0, Long.MAX_VALUE / 2, set(node1));
    Murmur3BulkTokenRange tr4 = range(Long.MAX_VALUE / 2, Long.MAX_VALUE, set(node1));
    // when
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    List<BulkTokenRange> groups = trc.group(list(tr1, tr2, tr3, tr4), 2, Integer.MAX_VALUE);
    // then
    assertThat(groups.size()).isEqualTo(2);
    assertThat(groups)
        .containsExactly(
            range(Long.MIN_VALUE, 0, set(node1)), range(0, Long.MAX_VALUE, set(node1)));
  }

  @Test
  void testMultipleEndpoints() {
    // given
    Murmur3BulkTokenRange tr1 = range(0, 10, set(node2, node1, node3));
    Murmur3BulkTokenRange tr2 = range(10, 20, set(node1, node3, node2));
    Murmur3BulkTokenRange tr3 = range(20, 30, set(node3, node1, node4));
    Murmur3BulkTokenRange tr4 = range(30, 40, set(node1, node3, node4));
    // when
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    List<BulkTokenRange> groups = trc.group(list(tr1, tr2, tr3, tr4), 1, Integer.MAX_VALUE);
    // then
    assertThat(groups.size()).isEqualTo(2);
    assertThat(groups.get(0)).hasRange(0L, 20L);
    assertThat(groups.get(0).replicas()).containsOnly(node1, node2, node3);
    assertThat(groups.get(1)).hasRange(20L, 40L);
    assertThat(groups.get(1).replicas()).containsOnly(node1, node3, node4);
  }

  @Test
  void testMaxGroupSize() {
    // given
    Murmur3BulkTokenRange tr1 = range(0, 10, set(node1, node2, node3));
    Murmur3BulkTokenRange tr2 = range(10, 20, set(node1, node2, node3));
    Murmur3BulkTokenRange tr3 = range(20, 30, set(node1, node2, node3));
    // when
    TokenRangeClusterer trc = new TokenRangeClusterer(tokenFactory);
    List<BulkTokenRange> groups = trc.group(list(tr1, tr2, tr3), 1, 1);
    // then
    assertThat(groups.size()).isEqualTo(3);
    assertThat(groups).containsExactly(tr1, tr2, tr3);
  }

  private static Murmur3BulkTokenRange range(long start, long end, Set<EndPoint> nodes) {
    return new Murmur3BulkTokenRange(newToken(start), newToken(end), nodes);
  }

  @SafeVarargs
  private static <T> Set<T> set(T... elements) {
    return Sets.newLinkedHashSet(elements);
  }

  @SafeVarargs
  private static <T> List<T> list(T... elements) {
    return Lists.newArrayList(elements);
  }
}
