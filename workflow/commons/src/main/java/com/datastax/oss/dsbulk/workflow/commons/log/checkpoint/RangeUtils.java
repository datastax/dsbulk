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

import java.util.List;

class RangeUtils {

  static void addPosition(List<Range> ranges, long position) {
    int low = 0;
    int high = ranges.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      Range midVal = ranges.get(mid);
      if (midVal.getUpper() + 1 < position) {
        low = mid + 1;
      } else if (midVal.getUpper() + 1 == position) {
        midVal.setUpper(position);
        if (mid < ranges.size() - 1) {
          Range nextVal = ranges.get(mid + 1);
          if (nextVal.getLower() - 1 == position) {
            ranges.remove(mid + 1);
            midVal.setUpper(nextVal.getUpper());
          }
        }
        return;
      } else if (midVal.getLower() - 1 > position) {
        high = mid - 1;
      } else if (midVal.getLower() - 1 == position) {
        midVal.setLower(position);
        if (mid > 0) {
          Range prevVal = ranges.get(mid - 1);
          if (prevVal.getUpper() + 1 == position) {
            ranges.remove(mid - 1);
            midVal.setLower(prevVal.getLower());
          }
        }
        return;
      } else {
        return; // midVal contains position
      }
    }
    ranges.add(low, new Range(position));
  }

  static void addRange(List<Range> ranges, Range toAdd) {
    int low = 0;
    int high = ranges.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      Range midVal = ranges.get(mid);
      if (toAdd.getLower() > midVal.getUpper() + 1L) {
        low = mid + 1;
      } else if (toAdd.getUpper() < midVal.getLower() - 1L) {
        high = mid - 1;
      } else if (midVal.isContiguous(toAdd)) {
        if (midVal.getUpper() < toAdd.getUpper()) {
          midVal.setUpper(toAdd.getUpper());
          mergeNext(ranges, mid, midVal);
        }
        if (midVal.getLower() > toAdd.getLower()) {
          midVal.setLower(toAdd.getLower());
          mergePrev(ranges, mid, midVal);
        }
        return;
      }
    }
    ranges.add(low, toAdd);
  }

  private static void mergePrev(List<Range> ranges, int i, Range r) {
    while (--i >= 0) {
      Range prev = ranges.get(i);
      if (prev.getUpper() >= r.getLower() - 1L) {
        ranges.remove(i);
        r.merge(prev);
      } else {
        break;
      }
    }
  }

  private static void mergeNext(List<Range> ranges, int i, Range r) {
    while (++i < ranges.size()) {
      Range next = ranges.get(i);
      if (next.getLower() <= r.getUpper() + 1L) {
        ranges.remove(i);
        r.merge(next);
      } else {
        break;
      }
    }
  }

  static boolean contains(List<Range> ranges, long position) {
    int low = 0;
    int high = ranges.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      Range midVal = ranges.get(mid);
      int cmp = midVal.getLower() > position ? 1 : midVal.getUpper() < position ? -1 : 0;
      if (cmp == 0) {
        return true;
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return false;
  }
}
