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
package com.datastax.oss.dsbulk.workflow.commons.log.checkpoint;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A set of non-contiguous ranges sorted by ascending order of their boundaries.
 *
 * <p>The set is modeled internally with a List to facilitate update and merge operations, but it is
 * guaranteed to be sorted and not to contain contiguous ranges. The mutating methods in {@link
 * RangeUtils} also enforce this invariant.
 */
public class RangeSet {

  @NonNull
  public static RangeSet parse(@NonNull String text) {
    if (text.isEmpty()) {
      return RangeSet.of();
    }
    List<Range> ranges =
        Pattern.compile(",").splitAsStream(text).map(Range::parse).collect(Collectors.toList());
    return RangeSet.of(ranges);
  }

  @NonNull
  public static RangeSet of(Range... ranges) {
    return of(Arrays.asList(ranges));
  }

  @NonNull
  public static RangeSet of(@NonNull Iterable<Range> ranges) {
    List<Range> sorted = new ArrayList<>();
    for (Range range : ranges) {
      RangeUtils.addRange(sorted, range);
    }
    return new RangeSet(sorted);
  }

  private final List<Range> ranges;

  public RangeSet() {
    this(new ArrayList<>());
  }

  private RangeSet(List<Range> ranges) {
    this.ranges = ranges;
  }

  public boolean contains(long position) {
    return RangeUtils.contains(ranges, position);
  }

  public void update(long position) {
    RangeUtils.addPosition(ranges, position);
  }

  public void merge(@NonNull RangeSet other) {
    for (Range range : other.ranges) {
      RangeUtils.addRange(ranges, range);
    }
  }

  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  public int size() {
    return ranges.size();
  }

  public long sum() {
    return ranges.stream().mapToLong(range -> range.getUpper() - range.getLower() + 1).sum();
  }

  public void clear() {
    ranges.clear();
  }

  @NonNull
  public Iterator<Range> iterator() {
    return ranges.iterator();
  }

  @NonNull
  public Stream<Range> stream() {
    return ranges.stream();
  }

  @NonNull
  public String asText() {
    return ranges.stream().map(Range::asText).collect(Collectors.joining(","));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RangeSet)) {
      return false;
    }
    RangeSet rangeSet = (RangeSet) o;
    return ranges.equals(rangeSet.ranges);
  }

  @Override
  public int hashCode() {
    return ranges.hashCode();
  }

  @Override
  public String toString() {
    return asText();
  }
}
