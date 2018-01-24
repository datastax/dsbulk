/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
