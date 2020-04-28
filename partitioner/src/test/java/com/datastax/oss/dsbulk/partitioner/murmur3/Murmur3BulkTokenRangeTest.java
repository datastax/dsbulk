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
package com.datastax.oss.dsbulk.partitioner.murmur3;

import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newToken;

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import java.math.BigInteger;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class Murmur3BulkTokenRangeTest {

  private final long minToken = Murmur3TokenFactory.MIN_TOKEN.getValue();

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
    assertThat(range(3, 3)).hasSize(Murmur3BulkTokenFactory.TOTAL_TOKEN_COUNT);
    assertThat(range(minToken, minToken)).hasSize(Murmur3BulkTokenFactory.TOTAL_TOKEN_COUNT);
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_report_fraction() {
    double tokenCount = Murmur3BulkTokenFactory.TOTAL_TOKEN_COUNT.doubleValue();
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

  private static Murmur3BulkTokenRange range(long start, long end) {
    return new Murmur3BulkTokenRange(newToken(start), newToken(end), Collections.emptySet());
  }
}
