/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.assertions;

import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.typesafe.config.Config;

public class CommonsAssertions extends org.assertj.core.api.Assertions {

  public static ConfigAssert assertThat(Config config) {
    return new ConfigAssert(config);
  }

  public static LogInterceptorAssert assertThat(LogInterceptor logInterceptor) {
    return new LogInterceptorAssert(logInterceptor);
  }
}
