/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.assertions;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ConvertingCodecAssert;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;

public class CommonsAssertions extends org.assertj.core.api.Assertions {

  public static ConfigAssert assertThat(Config config) {
    return new ConfigAssert(config);
  }

  public static LogInterceptorAssert assertThat(LogInterceptor logInterceptor) {
    return new LogInterceptorAssert(logInterceptor);
  }

  public static <K, V> MultimapAssert<K, V> assertThat(Multimap<K, V> map) {
    return new MultimapAssert<>(map);
  }

  public static <FROM, TO> ConvertingCodecAssert<FROM, TO> assertThat(
      ConvertingCodec<FROM, TO> actual) {
    return new ConvertingCodecAssert<>(actual);
  }
}
