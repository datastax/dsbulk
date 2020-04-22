/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.log;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PositionsTracker {

  private final Map<URI, List<Range>> positions = new HashMap<>();

  public Map<URI, List<Range>> getPositions() {
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
              positions = new ArrayList<>(100);
              positions.add(new Range(position));
              return positions;
            } else {
              return addPosition(positions, position);
            }
          });
    }
  }

  @NonNull
  private static List<Range> addPosition(@NonNull List<Range> positions, long position) {
    ListIterator<Range> iterator = positions.listIterator();
    while (iterator.hasNext()) {
      Range range = iterator.next();
      if (range.contains(position)) {
        return positions;
      } else if (range.getUpper() + 1L == position) {
        range.setUpper(position);
        if (iterator.hasNext()) {
          Range next = iterator.next();
          if (range.getUpper() == next.getLower() - 1L) {
            iterator.remove();
            range = iterator.previous();
            range.setUpper(next.getUpper());
          }
        }
        return positions;
      } else if (range.getLower() - 1L == position) {
        range.setLower(position);
        return positions;
      } else if (position < range.getLower()) {
        iterator.previous();
        iterator.add(new Range(position));
        return positions;
      }
    }
    iterator.add(new Range(position));
    return positions;
  }
}
