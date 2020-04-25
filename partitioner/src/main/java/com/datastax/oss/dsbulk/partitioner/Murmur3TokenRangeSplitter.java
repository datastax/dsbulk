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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

class Murmur3TokenRangeSplitter implements TokenRangeSplitter<Long, Token<Long>> {

  public static final Murmur3TokenRangeSplitter INSTANCE = new Murmur3TokenRangeSplitter();

  private Murmur3TokenRangeSplitter() {}

  @Override
  public List<TokenRange<Long, Token<Long>>> split(
      TokenRange<Long, Token<Long>> tokenRange, int splitCount) {
    BigInteger rangeSize = tokenRange.size();
    BigInteger val = BigInteger.valueOf(splitCount);
    // If the range size is lesser than the number of splits,
    // use the range size as number of splits and yield (size-of-range) splits of size 1
    BigInteger splitPointsCount = rangeSize.compareTo(val) < 0 ? rangeSize : val;
    BigInteger start = BigInteger.valueOf(tokenRange.start().value());
    List<Token<Long>> splitPoints = new ArrayList<>();
    for (BigInteger i = ZERO; i.compareTo(splitPointsCount) < 0; i = i.add(ONE)) {
      // instead of applying a fix increment we multiply and
      // divide again at each step to compensate for non-integral
      // increment sizes and thus to create splits of sizes as even as
      // possible (iow, to minimize the split sizes variance).
      BigInteger increment = rangeSize.multiply(i).divide(splitPointsCount);
      // DAT-334: use longValue() instead of longValueExact() to allow
      // long overflows (a long overflow here means that we wrap around the ring).
      Murmur3Token splitPoint = new Murmur3Token(start.add(increment).longValue());
      splitPoints.add(splitPoint);
    }
    splitPoints.add(tokenRange.end());
    List<TokenRange<Long, Token<Long>>> splits = new ArrayList<>();
    for (int i = 0; i < splitPoints.size() - 1; i++) {
      List<Token<Long>> window = splitPoints.subList(i, i + 2);
      TokenRange<Long, Token<Long>> split =
          new TokenRange<>(
              window.get(0), window.get(1), tokenRange.replicas(), tokenRange.tokenFactory());
      splits.add(split);
    }
    return splits;
  }
}
