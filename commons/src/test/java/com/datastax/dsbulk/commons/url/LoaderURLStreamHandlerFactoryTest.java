/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory.StdinStdoutUrlStreamHandler;
import org.junit.jupiter.api.Test;

class LoaderURLStreamHandlerFactoryTest {

  @Test
  void should_handle_std_protocol() throws Exception {
    LoaderURLStreamHandlerFactory factory = new LoaderURLStreamHandlerFactory();
    assertThat(factory.createURLStreamHandler("std"))
        .isNotNull()
        .isInstanceOf(StdinStdoutUrlStreamHandler.class);
    assertThat(factory.createURLStreamHandler("STD"))
        .isNotNull()
        .isInstanceOf(StdinStdoutUrlStreamHandler.class);
  }
}
