/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringUtilsTest {

  @Test
  void should_ensure_brackets() {
    assertThat(StringUtils.ensureBrackets("")).isEqualTo("[]");
    assertThat(StringUtils.ensureBrackets("[]")).isEqualTo("[]");
    assertThat(StringUtils.ensureBrackets("foo")).isEqualTo("[foo]");
    assertThat(StringUtils.ensureBrackets("[foo]")).isEqualTo("[foo]");
  }

  @Test
  void should_ensure_braces() {
    assertThat(StringUtils.ensureBraces("")).isEqualTo("{}");
    assertThat(StringUtils.ensureBraces("{}")).isEqualTo("{}");
    assertThat(StringUtils.ensureBraces("foo")).isEqualTo("{foo}");
    assertThat(StringUtils.ensureBraces("{foo}")).isEqualTo("{foo}");
  }

  @Test
  void should_ensure_quoted() {
    assertThat(StringUtils.ensureQuoted("")).isEqualTo("\"\"");
    assertThat(StringUtils.ensureQuoted("\"")).isEqualTo("\"");
    assertThat(StringUtils.ensureQuoted("\\\"")).isEqualTo("\"\\\"\"");
    assertThat(StringUtils.ensureQuoted("\"\"")).isEqualTo("\"\"");
    assertThat(StringUtils.ensureQuoted("foo")).isEqualTo("\"foo\"");
    assertThat(StringUtils.ensureQuoted("\"foo\"")).isEqualTo("\"foo\"");
  }

  @Test
  void should_quote_jmx_if_necessary() {
    assertThat(StringUtils.quoteJMXIfNecessary("")).isEqualTo("");
    assertThat(StringUtils.quoteJMXIfNecessary("foo")).isEqualTo("foo");
    assertThat(StringUtils.quoteJMXIfNecessary("foo?")).isEqualTo("\"foo\\?\"");
    assertThat(StringUtils.quoteJMXIfNecessary("foo*")).isEqualTo("\"foo\\*\"");
    assertThat(StringUtils.quoteJMXIfNecessary("foo\\")).isEqualTo("\"foo\\\\\"");
    assertThat(StringUtils.quoteJMXIfNecessary("foo\n")).isEqualTo("\"foo\\n\"");
  }
}
