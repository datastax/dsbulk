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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.dsbulk.commons.url.BulkLoaderURLStreamHandlerFactory.StdinStdoutUrlStreamHandler;
import org.junit.jupiter.api.Test;

class BulkLoaderURLStreamHandlerFactoryTest {

  @Test
  void should_handle_std_protocol() throws Exception {
    BulkLoaderURLStreamHandlerFactory factory = new BulkLoaderURLStreamHandlerFactory();
    assertThat(factory.createURLStreamHandler("std"))
        .isNotNull()
        .isInstanceOf(StdinStdoutUrlStreamHandler.class);
    assertThat(factory.createURLStreamHandler("STD"))
        .isNotNull()
        .isInstanceOf(StdinStdoutUrlStreamHandler.class);
  }
}
