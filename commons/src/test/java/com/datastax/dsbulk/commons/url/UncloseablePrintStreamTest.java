/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.url;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

class UncloseableOutputStreamTest {

  @Test
  void should_not_close_output_stream() throws Exception {
    ByteArrayOutputStream delegate = spy(new ByteArrayOutputStream());
    UncloseableOutputStream is = new UncloseableOutputStream(delegate);
    is.close();
    verify(delegate, never()).close();
  }
}
