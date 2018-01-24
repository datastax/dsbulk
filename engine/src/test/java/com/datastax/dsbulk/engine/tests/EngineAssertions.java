/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests;

import com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodecAssert;

public class EngineAssertions extends CommonsAssertions {

  public static <FROM, TO> ConvertingCodecAssert<FROM, TO> assertThat(
      ConvertingCodec<FROM, TO> actual) {
    return new ConvertingCodecAssert<>(actual);
  }
}
