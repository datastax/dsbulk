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
package com.datastax.oss.dsbulk.partitioner.random;

import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;
import static java.math.BigInteger.ZERO;

import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory;
import java.math.BigInteger;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class RandomBulkTokenRangeTest {

  private final BigInteger minToken = RandomTokenFactory.MIN_TOKEN.getValue();
  private final BigInteger maxToken = RandomTokenFactory.MAX_TOKEN.getValue();

  @Test
  void should_report_size() {
    assertThat(range(3, 9)).hasSize(BigInteger.valueOf(6));
    assertThat(range(9, 3))
        .hasSize(RandomBulkTokenFactory.TOTAL_TOKEN_COUNT.subtract(BigInteger.valueOf(6)));
    assertThat(range(minToken, 3)).hasSize(BigInteger.valueOf(3).subtract(minToken));
    assertThat(range(3, minToken))
        .hasSize(
            RandomBulkTokenFactory.TOTAL_TOKEN_COUNT.subtract(BigInteger.valueOf(3)).add(minToken));
    assertThat(range(3, 3)).hasSize(RandomBulkTokenFactory.TOTAL_TOKEN_COUNT);
    assertThat(range(0, 0)).hasSize(RandomBulkTokenFactory.TOTAL_TOKEN_COUNT);
    assertThat(range(minToken, minToken)).hasSize(RandomBulkTokenFactory.TOTAL_TOKEN_COUNT);
    assertThat(range(maxToken, minToken)).hasSize(ZERO);
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_report_fraction() {
    assertThat(range(0, 0)).hasFraction(1.0);
    assertThat(range(0, maxToken)).hasFraction(1.0);
    assertThat(range(0, maxToken.divide(BigInteger.valueOf(2)))).hasFraction(0.5);
    assertThat(range(3, 3)).hasFraction(1.0);
    assertThat(range(minToken, minToken)).hasFraction(1.0);
    assertThat(range(maxToken, minToken)).hasFraction(0.0);
  }

  private static RandomBulkTokenRange range(long start, long end) {
    return range(BigInteger.valueOf(start), BigInteger.valueOf(end));
  }

  private static RandomBulkTokenRange range(BigInteger start, long end) {
    return range(start, BigInteger.valueOf(end));
  }

  private static RandomBulkTokenRange range(long start, BigInteger end) {
    return range(BigInteger.valueOf(start), end);
  }

  private static RandomBulkTokenRange range(BigInteger start, BigInteger end) {
    return new RandomBulkTokenRange(
        new RandomToken(start), new RandomToken(end), Collections.emptySet());
  }
}
