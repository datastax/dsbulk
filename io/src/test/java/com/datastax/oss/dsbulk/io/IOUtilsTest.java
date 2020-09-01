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
package com.datastax.oss.dsbulk.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.dsbulk.url.BulkLoaderURLStreamHandlerFactory;
import java.net.MalformedURLException;
import java.net.URL;
import org.junit.jupiter.api.Test;

class IOUtilsTest {

  static {
    BulkLoaderURLStreamHandlerFactory.install();
  }

  @Test
  void should_detect_standard_stream_url() throws MalformedURLException {
    assertThat(IOUtils.isStandardStream(new URL("http://acme.com"))).isFalse();
    assertThat(IOUtils.isStandardStream(new URL("std:/"))).isTrue();
  }
}
