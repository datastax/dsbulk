/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.url;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MainURLStreamHandlerFactoryTest {

  @Test
  public void should_handle_stdin_protocol() throws Exception {
    MainURLStreamHandlerFactory factory = new MainURLStreamHandlerFactory();
    assertThat(factory.createURLStreamHandler("stdin"))
        .isNotNull()
        .isInstanceOf(StandardInputURLStreamHandler.class);
    assertThat(factory.createURLStreamHandler("STDIN"))
        .isNotNull()
        .isInstanceOf(StandardInputURLStreamHandler.class);
  }
}
