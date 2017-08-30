/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import org.junit.Test;

public class UncloseableOutputStreamTest {

  @Test
  public void should_not_close_output_stream() throws Exception {
    ByteArrayOutputStream delegate = spy(new ByteArrayOutputStream());
    UncloseableOutputStream is = new UncloseableOutputStream(delegate);
    is.close();
    verify(delegate, never()).close();
  }
}
