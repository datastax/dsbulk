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

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;

import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory;
import com.datastax.oss.dsbulk.partitioner.BulkTokenRange;
import com.datastax.oss.dsbulk.partitioner.TokenRangeSplitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class RandomTokenRangeSplitter implements TokenRangeSplitter {

  @Override
  public @NonNull List<BulkTokenRange> split(@NonNull BulkTokenRange tokenRange, int splitCount) {
    BigInteger rangeSize = tokenRange.size();
    BigInteger val = BigInteger.valueOf(splitCount);
    // If the range size is lesser than the number of splits,
    // use the range size as number of splits and yield (size-of-range) splits of size 1
    BigInteger splitPointsCount = rangeSize.compareTo(val) < 0 ? rangeSize : val;
    BigInteger start = ((RandomToken) tokenRange.getStart()).getValue();
    List<RandomToken> splitPoints = new ArrayList<>();
    for (BigInteger i = ZERO; i.compareTo(splitPointsCount) < 0; i = i.add(ONE)) {
      // instead of applying a fix increment we multiply and
      // divide again at each step to compensate for non-integral
      // increment sizes and thus to create splits of sizes as even as
      // possible (iow, to minimize the split sizes variance).
      BigInteger increment = rangeSize.multiply(i).divide(splitPointsCount);
      RandomToken splitPoint = new RandomToken(wrap(start.add(increment)));
      splitPoints.add(splitPoint);
    }
    splitPoints.add((RandomToken) tokenRange.getEnd());
    List<BulkTokenRange> splits = new ArrayList<>();
    for (int i = 0; i < splitPoints.size() - 1; i++) {
      List<RandomToken> window = splitPoints.subList(i, i + 2);
      RandomBulkTokenRange split =
          new RandomBulkTokenRange(window.get(0), window.get(1), tokenRange.replicas());
      splits.add(split);
    }
    return splits;
  }

  private BigInteger wrap(BigInteger token) {
    return token.compareTo(RandomTokenFactory.MAX_TOKEN.getValue()) <= 0
        ? token
        : token.subtract(RandomTokenFactory.MAX_TOKEN.getValue());
  }
}
