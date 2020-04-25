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

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class Murmur3TokenFactoryTest {

  private Murmur3TokenFactory factory = Murmur3TokenFactory.INSTANCE;

  @Test
  void should_create_token_from_string() {
    assertThat(factory.tokenFromString("0")).isEqualTo(new Murmur3Token(0L));
    assertThat(factory.tokenFromString("-1")).isEqualTo(new Murmur3Token(-1L));
    assertThat(factory.tokenFromString(Long.toString(Long.MAX_VALUE)))
        .isEqualTo(factory.maxToken());
    assertThat(factory.tokenFromString(Long.toString(Long.MIN_VALUE)))
        .isEqualTo(factory.minToken());
  }

  @Test
  void should_calculate_distance_between_tokens_if_right_gt_left() {
    assertThat(factory.distance(new Murmur3Token(0L), new Murmur3Token(1L))).isEqualTo(ONE);
  }

  @Test
  void should_calculate_distance_between_tokens_if_right_lte_left() {
    assertThat(factory.distance(new Murmur3Token(0L), new Murmur3Token(0L)))
        .isEqualTo(factory.totalTokenCount());
    assertThat(factory.distance(factory.maxToken(), factory.minToken())).isEqualTo(ZERO);
  }

  @Test
  void should_calculate_ring_fraction() {
    assertThat(factory.fraction(new Murmur3Token(0L), new Murmur3Token(0L))).isEqualTo(1.0);
    assertThat(factory.fraction(new Murmur3Token(0L), factory.maxToken())).isEqualTo(0.5);
    assertThat(factory.fraction(factory.maxToken(), new Murmur3Token(0L))).isEqualTo(0.5);
  }
}
