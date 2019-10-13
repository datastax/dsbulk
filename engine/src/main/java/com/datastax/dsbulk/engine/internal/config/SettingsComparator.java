/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.config;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * String comparator that supports placing "high priority" values first. This allows a setting group
 * to have "mostly" alpha-sorted settings, but with certain settings promoted to be first (and thus
 * emitted first when generating documentation).
 */
class SettingsComparator implements Comparator<String> {

  private final Map<String, Integer> prioritizedValues;

  SettingsComparator(String... highPriorityValues) {
    this(Arrays.asList(highPriorityValues));
  }

  SettingsComparator(List<String> highPriorityValues) {
    prioritizedValues = new HashMap<>();
    int counter = 0;
    for (String s : highPriorityValues) {
      prioritizedValues.put(s, counter++);
    }
  }

  @Override
  public int compare(String left, String right) {
    Integer leftInd = Integer.MAX_VALUE;
    Integer rightInd = Integer.MAX_VALUE;
    for (String value : prioritizedValues.keySet()) {
      String valueWithDot = value + ".";
      if (left.startsWith(valueWithDot) || left.equals(value)) {
        leftInd = prioritizedValues.get(value);
      }
      if (right.startsWith(valueWithDot) || right.equals(value)) {
        rightInd = prioritizedValues.get(value);
      }
    }
    int indCompare = leftInd.compareTo(rightInd);
    return indCompare != 0 ? indCompare : left.compareTo(right);
  }
}
