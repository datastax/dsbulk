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

import java.math.BigInteger;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TokenRangeTest {

  private long minToken = Murmur3TokenFactory.INSTANCE.minToken().value();

  @Test
  void should_report_boundaries() {
    assertThat(range(9, 3)).startsWith(9L).endsWith(3L);
    assertThat(range(3, 9)).startsWith(3L).endsWith(9L);
    assertThat(range(3, minToken)).startsWith(3L).endsWith(minToken);
    assertThat(range(minToken, 3)).startsWith(minToken).endsWith(3L);
    assertThat(range(3, 3)).startsWith(3L).endsWith(3L);
    assertThat(range(minToken, minToken)).startsWith(minToken).endsWith(minToken);
  }

  @Test
  void should_report_size() {
    assertThat(range(9, 3)).hasSize(new BigInteger("18446744073709551609"));
    assertThat(range(3, 9)).hasSize(BigInteger.valueOf(6));
    assertThat(range(3, minToken)).hasSize(new BigInteger("9223372036854775804"));
    assertThat(range(minToken, 3)).hasSize(new BigInteger("9223372036854775811"));
    assertThat(range(3, 3)).hasSize(Murmur3TokenFactory.INSTANCE.totalTokenCount());
    assertThat(range(minToken, minToken)).hasSize(Murmur3TokenFactory.INSTANCE.totalTokenCount());
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_report_fraction() {
    double tokenCount = Murmur3TokenFactory.INSTANCE.totalTokenCount().doubleValue();
    assertThat(range(9, 3)).hasFraction(18446744073709551609d / tokenCount);
    assertThat(range(3, 9)).hasFraction(6 / tokenCount);
    assertThat(range(3, minToken)).hasFraction(9223372036854775804d / tokenCount);
    assertThat(range(minToken, 3)).hasFraction(9223372036854775811d / tokenCount);
    assertThat(range(3, 3)).hasFraction(1.0);
    assertThat(range(minToken, minToken)).hasFraction(1.0);
  }

  @Test
  void should_unwrap_to_non_wrapping_ranges() {
    assertThat(range(9, 3))
        .isNotEmpty()
        .isWrappedAround()
        .unwrapsTo(range(9, minToken), range(minToken, 3));
    assertThat(range(3, 9)).isNotEmpty().isNotWrappedAround();
    assertThat(range(3, minToken)).isNotEmpty().isNotWrappedAround();
    assertThat(range(minToken, 3)).isNotEmpty().isNotWrappedAround();
    assertThat(range(3, 3)).isEmpty().isNotWrappedAround();
    assertThat(range(minToken, minToken)).isNotWrappedAround();
  }

  private static TokenRange<Long, Token<Long>> range(long start, long end) {
    return new TokenRange<>(
        Murmur3TokenFactory.INSTANCE.tokenFromString(Long.toString(start)),
        Murmur3TokenFactory.INSTANCE.tokenFromString(Long.toString(end)),
        Collections.emptySet(),
        Murmur3TokenFactory.INSTANCE);
  }
}
