/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests.utils;

import static com.datastax.dsbulk.commons.tests.utils.Version.isWithinRange;
import static com.datastax.dsbulk.commons.tests.utils.Version.parse;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** */
class VersionTest {

  @Test
  void should_analyze_ranges() {
    assertThat(isWithinRange(null, null, parse("5.1.0"))).isTrue();
    assertThat(isWithinRange(parse("5.1.0"), null, parse("5.1.0"))).isTrue();
    assertThat(isWithinRange(null, parse("5.1.0"), parse("5.1.0"))).isFalse();
    assertThat(isWithinRange(parse("5.0.0"), parse("5.1.0"), parse("5.1.0"))).isFalse();
    assertThat(isWithinRange(parse("5.0.0"), parse("5.1.0"), parse("5.0.0"))).isTrue();
    assertThat(isWithinRange(parse("5.0.0"), parse("5.1.0"), parse("5.0.9"))).isTrue();
    assertThat(isWithinRange(parse("5.0.0"), parse("5.1.0"), parse("4.9.0"))).isFalse();
    assertThat(isWithinRange(parse("5.0.0"), parse("5.1.0"), parse("5.1.1"))).isFalse();
  }
}
