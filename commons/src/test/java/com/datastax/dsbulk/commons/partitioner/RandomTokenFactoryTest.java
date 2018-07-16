/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class RandomTokenFactoryTest {

  private RandomTokenFactory factory = RandomTokenFactory.INSTANCE;

  @Test
  void should_create_token_from_string() {
    assertThat(factory.tokenFromString("0")).isEqualTo(new RandomToken(ZERO));
    assertThat(factory.tokenFromString("-1")).isEqualTo(new RandomToken(BigInteger.valueOf(-1)));
    assertThat(factory.tokenFromString("170141183460469231731687303715884105728"))
        .isEqualTo(new RandomToken(new BigInteger("170141183460469231731687303715884105728")));
  }

  @Test
  void should_calculate_distance_between_tokens_if_right_gt_left() {
    assertThat(factory.distance(new RandomToken(ZERO), new RandomToken(ONE))).isEqualTo(ONE);
  }

  @Test
  void should_calculate_distance_between_tokens_if_right_lte_left() {
    assertThat(factory.distance(new RandomToken(ZERO), new RandomToken(ZERO)))
        .isEqualTo(factory.totalTokenCount());
    assertThat(factory.distance(factory.maxToken(), factory.minToken())).isEqualTo(ZERO);
  }

  @Test
  void should_calculate_ring_fraction() {
    assertThat(factory.fraction(new RandomToken(ZERO), new RandomToken(ZERO))).isEqualTo(1.0);
    assertThat(factory.fraction(new RandomToken(ZERO), factory.maxToken())).isEqualTo(1.0);
    assertThat(factory.fraction(factory.maxToken(), factory.minToken())).isEqualTo(0.0);
    assertThat(
            factory.fraction(
                new RandomToken(ZERO),
                new RandomToken(factory.maxToken().value().divide(BigInteger.valueOf(2)))))
        .isEqualTo(0.5);
  }
}
