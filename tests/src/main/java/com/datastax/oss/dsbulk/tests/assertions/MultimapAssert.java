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
package com.datastax.oss.dsbulk.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import org.assertj.core.api.AbstractObjectAssert;

@SuppressWarnings("UnusedReturnValue")
public class MultimapAssert<K, V>
    extends AbstractObjectAssert<MultimapAssert<K, V>, Multimap<K, V>> {

  MultimapAssert(Multimap<K, V> map) {
    super(map, MultimapAssert.class);
  }

  public MultimapAssert<K, V> containsEntry(K key, V value) {
    assertThat(actual.containsEntry(key, value))
        .overridingErrorMessage(
            "Expecting %s to have the entry %s -> %s but it did not", actual, key, value)
        .isTrue();
    return this;
  }

  public MultimapAssert<K, V> isEmpty() {
    assertThat(actual.isEmpty())
        .overridingErrorMessage("Expecting %s to be empty but it was not", actual)
        .isTrue();
    return this;
  }

  public MultimapAssert<K, V> hasSize(int expected) {
    assertThat(actual.size())
        .overridingErrorMessage(
            "Expecting %s to have size %d but it had size %d", actual, expected, actual.size())
        .isEqualTo(expected);
    return this;
  }
}
