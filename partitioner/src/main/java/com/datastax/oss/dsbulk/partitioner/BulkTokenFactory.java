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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.random.RandomBulkTokenFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.Set;

public interface BulkTokenFactory extends TokenFactory {

  @NonNull
  static BulkTokenFactory forPartitioner(@NonNull String partitioner) {
    if (partitioner.equals(Murmur3TokenFactory.PARTITIONER_NAME)) {
      return new Murmur3BulkTokenFactory();
    } else if (partitioner.equals(RandomTokenFactory.PARTITIONER_NAME)) {
      return new RandomBulkTokenFactory();
    } else {
      throw new IllegalArgumentException("Unknown partitioner: " + partitioner);
    }
  }

  /** @return Total token count in a ring. */
  @NonNull
  BigInteger totalTokenCount();

  /** Creates a {@link BulkTokenRange} for the given start and end tokens. */
  @NonNull
  BulkTokenRange range(@NonNull Token start, @NonNull Token end, @NonNull Set<EndPoint> replicas);

  /** Returns a {@link TokenRangeSplitter} for the type of tokens managed by this token factory. */
  @NonNull
  TokenRangeSplitter splitter();

  /** Returns a {@link TokenRangeClusterer} for the type of tokens managed by this token factory. */
  @NonNull
  TokenRangeClusterer clusterer();
}
