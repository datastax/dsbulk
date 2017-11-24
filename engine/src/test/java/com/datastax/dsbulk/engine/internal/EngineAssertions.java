/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal;

import com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodecAssert;

public class EngineAssertions extends CommonsAssertions {

  public static <FROM, TO> ConvertingCodecAssert<FROM, TO> assertThat(
      ConvertingCodec<FROM, TO> actual) {
    return new ConvertingCodecAssert<>(actual);
  }
}
