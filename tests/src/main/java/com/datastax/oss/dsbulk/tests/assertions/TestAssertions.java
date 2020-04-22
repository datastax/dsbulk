/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.assertions;

import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.typesafe.config.Config;
import org.assertj.core.api.Assertions;

public class TestAssertions extends Assertions {

  public static ConfigAssert assertThat(Config config) {
    return new ConfigAssert(config);
  }

  public static <K, V> MultimapAssert<K, V> assertThat(Multimap<K, V> map) {
    return new MultimapAssert<>(map);
  }

  public static LogInterceptorAssert assertThat(LogInterceptor logInterceptor) {
    return new LogInterceptorAssert(logInterceptor);
  }

  public static <FROM, TO> ConvertingCodecAssert<FROM, TO> assertThat(
      ConvertingCodec<FROM, TO> actual) {
    return new ConvertingCodecAssert<>(actual);
  }
}
