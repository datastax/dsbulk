/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

class TokenRangeClustererTest {

  private EndPoint node1 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.1", 9042));
  private EndPoint node2 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.2", 9042));
  private EndPoint node3 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.3", 9042));
  private EndPoint node4 =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("192.168.123.4", 9042));

  @Test
  void testEmpty() {
    // given
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(10);
    // when
    List<TokenRange<Long, Token<Long>>> groups = trc.group(Collections.emptyList());
    // then
    assertThat(groups.size()).isZero();
  }

  @Test
  void testTrivialClustering() {
    // given
    TokenRange<Long, Token<Long>> tr1 = range(0, 10, set(node1));
    TokenRange<Long, Token<Long>> tr2 = range(10, 20, set(node1));
    // when
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(1);
    List<TokenRange<Long, Token<Long>>> groups = trc.group(list(tr1, tr2));
    // then
    assertThat(groups.size()).isOne();
    assertThat(groups.get(0)).startsWith(tr1.start()).endsWith(tr2.end());
  }

  @Test
  void testSplitByHost() {
    // given
    TokenRange<Long, Token<Long>> tr1 = range(0, 10, set(node1));
    TokenRange<Long, Token<Long>> tr2 = range(10, 20, set(node1));
    TokenRange<Long, Token<Long>> tr3 = range(20, 30, set(node2));
    TokenRange<Long, Token<Long>> tr4 = range(30, 40, set(node2));
    // when
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(1);
    List<TokenRange<Long, Token<Long>>> groups = trc.group(list(tr1, tr2, tr3, tr4));
    // then
    assertThat(groups.size()).isEqualTo(2);
    Assertions.assertThat(groups)
        .containsExactly(range(0, 20, set(node1)), range(20, 40, set(node2)));
  }

  @Test
  void testSplitByCount() {
    // given
    TokenRange<Long, Token<Long>> tr1 = range(Long.MIN_VALUE, Long.MIN_VALUE / 2, set(node1));
    TokenRange<Long, Token<Long>> tr2 = range(Long.MIN_VALUE / 2, 0, set(node1));
    TokenRange<Long, Token<Long>> tr3 = range(0, Long.MAX_VALUE / 2, set(node1));
    TokenRange<Long, Token<Long>> tr4 = range(Long.MAX_VALUE / 2, Long.MAX_VALUE, set(node1));
    // when
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(2);
    List<TokenRange<Long, Token<Long>>> groups = trc.group(list(tr1, tr2, tr3, tr4));
    // then
    assertThat(groups.size()).isEqualTo(2);
    Assertions.assertThat(groups)
        .containsExactly(
            range(Long.MIN_VALUE, 0, set(node1)), range(0, Long.MAX_VALUE, set(node1)));
  }

  @Test
  void testMultipleEndpoints() {
    // given
    TokenRange<Long, Token<Long>> tr1 = range(0, 10, set(node2, node1, node3));
    TokenRange<Long, Token<Long>> tr2 = range(10, 20, set(node1, node3, node2));
    TokenRange<Long, Token<Long>> tr3 = range(20, 30, set(node3, node1, node4));
    TokenRange<Long, Token<Long>> tr4 = range(30, 40, set(node1, node3, node4));
    // when
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(1);
    List<TokenRange<Long, Token<Long>>> groups = trc.group(list(tr1, tr2, tr3, tr4));
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
    TokenRange<Long, Token<Long>> tr1 = range(0, 10, set(node1, node2, node3));
    TokenRange<Long, Token<Long>> tr2 = range(10, 20, set(node1, node2, node3));
    TokenRange<Long, Token<Long>> tr3 = range(20, 30, set(node1, node2, node3));
    // when
    TokenRangeClusterer<Long, Token<Long>> trc = new TokenRangeClusterer<>(1, 1);
    List<TokenRange<Long, Token<Long>>> groups = trc.group(list(tr1, tr2, tr3));
    // then
    assertThat(groups.size()).isEqualTo(3);
    Assertions.assertThat(groups).containsExactly(tr1, tr2, tr3);
  }

  private static TokenRange<Long, Token<Long>> range(long start, long end, Set<EndPoint> nodes) {
    Token<Long> startToken = new Murmur3Token(start);
    Token<Long> endToken = new Murmur3Token(end);
    return new TokenRange<>(startToken, endToken, nodes, Murmur3TokenFactory.INSTANCE);
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
