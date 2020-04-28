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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.dsbulk.partitioner.BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.BulkTokenRange;
import com.datastax.oss.dsbulk.partitioner.TokenRangeClusterer;
import com.datastax.oss.dsbulk.partitioner.TokenRangeSplitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.Set;

/** A {@link BulkTokenFactory} for the Murmur3 Partitioner. */
public class Murmur3BulkTokenFactory extends Murmur3TokenFactory implements BulkTokenFactory {

  public static final BigInteger TOTAL_TOKEN_COUNT =
      BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.valueOf(Long.MIN_VALUE));

  @NonNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NonNull
  @Override
  public BulkTokenRange range(
      @NonNull Token start, @NonNull Token end, @NonNull Set<EndPoint> replicas) {
    return new Murmur3BulkTokenRange(((Murmur3Token) start), (Murmur3Token) end, replicas);
  }

  @NonNull
  @Override
  public TokenRangeSplitter splitter() {
    return new Murmur3TokenRangeSplitter();
  }

  @NonNull
  @Override
  public TokenRangeClusterer clusterer() {
    return new TokenRangeClusterer(this);
  }
}
