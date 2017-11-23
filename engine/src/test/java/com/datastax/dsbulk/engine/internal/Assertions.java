/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodecAssert;
import com.datastax.dsbulk.engine.internal.settings.ConfigAssert;
import com.typesafe.config.Config;

public class Assertions extends org.assertj.core.api.Assertions {
  public static ConfigAssert assertThat(Config config) {
    return new ConfigAssert(config);
  }

  public static <FROM, TO> ConvertingCodecAssert<FROM, TO> assertThat(
      ConvertingCodec<FROM, TO> actual) {
    return new ConvertingCodecAssert<>(actual);
  }
}
