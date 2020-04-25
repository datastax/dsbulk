/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
