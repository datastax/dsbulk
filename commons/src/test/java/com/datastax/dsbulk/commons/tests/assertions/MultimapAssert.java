/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Multimap;
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
}
