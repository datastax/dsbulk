/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.commons.url;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import org.junit.jupiter.api.Test;

class UncloseableInputStreamTest {

  @Test
  void should_not_close_input_stream() throws Exception {
    ByteArrayInputStream delegate = spy(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    UncloseableInputStream is = new UncloseableInputStream(delegate);
    is.close();
    verify(delegate, never()).close();
  }
}
