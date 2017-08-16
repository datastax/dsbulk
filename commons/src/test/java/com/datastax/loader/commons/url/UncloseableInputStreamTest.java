/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.commons.url;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import org.junit.Test;

public class UncloseableInputStreamTest {

  @Test
  public void should_not_close_input_stream() throws Exception {
    ByteArrayInputStream delegate = spy(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    UncloseableInputStream is = new UncloseableInputStream(delegate);
    is.close();
    verify(delegate, never()).close();
  }
}
