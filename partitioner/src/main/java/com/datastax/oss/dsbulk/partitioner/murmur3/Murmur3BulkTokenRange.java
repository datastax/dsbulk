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
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.partitioner.BulkTokenRange;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.Set;

public class Murmur3BulkTokenRange extends Murmur3TokenRange implements BulkTokenRange {

  private final Set<EndPoint> replicas;

  public Murmur3BulkTokenRange(
      @NonNull Murmur3Token start, @NonNull Murmur3Token end, @NonNull Set<EndPoint> replicas) {
    super(start, end);
    this.replicas = ImmutableSet.copyOf(replicas);
  }

  @NonNull
  @Override
  public Murmur3Token getStart() {
    return (Murmur3Token) super.getStart();
  }

  @NonNull
  @Override
  public Murmur3Token getEnd() {
    return (Murmur3Token) super.getEnd();
  }

  @Override
  @NonNull
  public Set<EndPoint> replicas() {
    return replicas;
  }

  @Override
  @NonNull
  public BigInteger size() {
    BigInteger left = BigInteger.valueOf(getStart().getValue());
    BigInteger right = BigInteger.valueOf(getEnd().getValue());
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(Murmur3BulkTokenFactory.TOTAL_TOKEN_COUNT);
    }
  }

  @Override
  public double fraction() {
    return size().doubleValue() / Murmur3BulkTokenFactory.TOTAL_TOKEN_COUNT.doubleValue();
  }
}
