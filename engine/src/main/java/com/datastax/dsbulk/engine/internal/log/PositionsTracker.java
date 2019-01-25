/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import com.google.common.collect.Range;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class PositionsTracker {

  private final Map<URI, List<Range<Long>>> positions = new HashMap<>();

  public Map<URI, List<Range<Long>>> getPositions() {
    return positions;
  }

  public boolean isEmpty() {
    return positions.isEmpty();
  }

  public void update(URI resource, long position) {
    if (position > 0) {
      positions.compute(
          resource,
          (res, positions) -> {
            if (positions == null) {
              positions = new ArrayList<>();
              positions.add(Range.singleton(position));
              return positions;
            } else {
              return addPosition(positions, position);
            }
          });
    }
  }

  @NotNull
  private static List<Range<Long>> addPosition(
      @NotNull List<Range<Long>> positions, long position) {
    ListIterator<Range<Long>> iterator = positions.listIterator();
    while (iterator.hasNext()) {
      Range<Long> range = iterator.next();
      if (range.contains(position)) {
        return positions;
      } else if (range.upperEndpoint() + 1L == position) {
        range = Range.closed(range.lowerEndpoint(), position);
        iterator.set(range);
        if (iterator.hasNext()) {
          Range<Long> next = iterator.next();
          if (range.upperEndpoint() == next.lowerEndpoint() - 1) {
            iterator.remove();
            iterator.previous();
            iterator.set(Range.closed(range.lowerEndpoint(), next.upperEndpoint()));
          }
        }
        return positions;
      } else if (range.lowerEndpoint() - 1L == position) {
        range = Range.closed(position, range.upperEndpoint());
        iterator.set(range);
        return positions;
      } else if (position < range.lowerEndpoint()) {
        iterator.previous();
        iterator.add(Range.singleton(position));
        return positions;
      }
    }
    iterator.add(Range.singleton(position));
    return positions;
  }
}
