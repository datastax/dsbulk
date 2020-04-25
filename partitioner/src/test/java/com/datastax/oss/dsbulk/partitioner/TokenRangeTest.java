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

import com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions;
import java.math.BigInteger;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TokenRangeTest {

  private long minToken = Murmur3TokenFactory.INSTANCE.minToken().value();

  @Test
  void should_report_boundaries() {
    PartitionerAssertions.assertThat(range(9, 3)).startsWith(9L).endsWith(3L);
    PartitionerAssertions.assertThat(range(3, 9)).startsWith(3L).endsWith(9L);
    PartitionerAssertions.assertThat(range(3, minToken)).startsWith(3L).endsWith(minToken);
    PartitionerAssertions.assertThat(range(minToken, 3)).startsWith(minToken).endsWith(3L);
    PartitionerAssertions.assertThat(range(3, 3)).startsWith(3L).endsWith(3L);
    PartitionerAssertions.assertThat(range(minToken, minToken))
        .startsWith(minToken)
        .endsWith(minToken);
  }

  @Test
  void should_report_size() {
    PartitionerAssertions.assertThat(range(9, 3)).hasSize(new BigInteger("18446744073709551609"));
    PartitionerAssertions.assertThat(range(3, 9)).hasSize(BigInteger.valueOf(6));
    PartitionerAssertions.assertThat(range(3, minToken))
        .hasSize(new BigInteger("9223372036854775804"));
    PartitionerAssertions.assertThat(range(minToken, 3))
        .hasSize(new BigInteger("9223372036854775811"));
    PartitionerAssertions.assertThat(range(3, 3))
        .hasSize(Murmur3TokenFactory.INSTANCE.totalTokenCount());
    PartitionerAssertions.assertThat(range(minToken, minToken))
        .hasSize(Murmur3TokenFactory.INSTANCE.totalTokenCount());
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_report_fraction() {
    double tokenCount = Murmur3TokenFactory.INSTANCE.totalTokenCount().doubleValue();
    PartitionerAssertions.assertThat(range(9, 3)).hasFraction(18446744073709551609d / tokenCount);
    PartitionerAssertions.assertThat(range(3, 9)).hasFraction(6 / tokenCount);
    PartitionerAssertions.assertThat(range(3, minToken))
        .hasFraction(9223372036854775804d / tokenCount);
    PartitionerAssertions.assertThat(range(minToken, 3))
        .hasFraction(9223372036854775811d / tokenCount);
    PartitionerAssertions.assertThat(range(3, 3)).hasFraction(1.0);
    PartitionerAssertions.assertThat(range(minToken, minToken)).hasFraction(1.0);
  }

  @Test
  void should_unwrap_to_non_wrapping_ranges() {
    PartitionerAssertions.assertThat(range(9, 3))
        .isNotEmpty()
        .isWrappedAround()
        .unwrapsTo(range(9, minToken), range(minToken, 3));
    PartitionerAssertions.assertThat(range(3, 9)).isNotEmpty().isNotWrappedAround();
    PartitionerAssertions.assertThat(range(3, minToken)).isNotEmpty().isNotWrappedAround();
    PartitionerAssertions.assertThat(range(minToken, 3)).isNotEmpty().isNotWrappedAround();
    PartitionerAssertions.assertThat(range(3, 3)).isEmpty().isNotWrappedAround();
    PartitionerAssertions.assertThat(range(minToken, minToken)).isNotWrappedAround();
  }

  private static TokenRange<Long, Token<Long>> range(long start, long end) {
    return new TokenRange<>(
        Murmur3TokenFactory.INSTANCE.tokenFromString(Long.toString(start)),
        Murmur3TokenFactory.INSTANCE.tokenFromString(Long.toString(end)),
        Collections.emptySet(),
        Murmur3TokenFactory.INSTANCE);
  }
}
