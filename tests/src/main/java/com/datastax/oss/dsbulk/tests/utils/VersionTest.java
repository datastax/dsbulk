/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.utils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class VersionTest {

  @Test
  void should_analyze_ranges() {
    Assertions.assertThat(Version.isWithinRange(null, null, Version.parse("5.1.0"))).isTrue();
    Assertions.assertThat(
            Version.isWithinRange(Version.parse("5.1.0"), null, Version.parse("5.1.0")))
        .isTrue();
    Assertions.assertThat(
            Version.isWithinRange(null, Version.parse("5.1.0"), Version.parse("5.1.0")))
        .isFalse();
    Assertions.assertThat(
            Version.isWithinRange(
                Version.parse("5.0.0"), Version.parse("5.1.0"), Version.parse("5.1.0")))
        .isFalse();
    Assertions.assertThat(
            Version.isWithinRange(
                Version.parse("5.0.0"), Version.parse("5.1.0"), Version.parse("5.0.0")))
        .isTrue();
    Assertions.assertThat(
            Version.isWithinRange(
                Version.parse("5.0.0"), Version.parse("5.1.0"), Version.parse("5.0.9")))
        .isTrue();
    Assertions.assertThat(
            Version.isWithinRange(
                Version.parse("5.0.0"), Version.parse("5.1.0"), Version.parse("4.9.0")))
        .isFalse();
    Assertions.assertThat(
            Version.isWithinRange(
                Version.parse("5.0.0"), Version.parse("5.1.0"), Version.parse("5.1.1")))
        .isFalse();
  }
}
