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
package com.datastax.oss.dsbulk.url;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class BulkLoaderURLStreamHandlerFactoryTest {

  @Test
  void should_handle_installed_handlers() {
    Config config = mock(Config.class);
    when(config.hasPath("dsbulk.s3.region")).thenReturn(true);
    when(config.getString("dsbulk.s3.region")).thenReturn("us-west-1");
    when(config.hasPath("dsbulk.s3.profile")).thenReturn(true);
    when(config.getString("dsbulk.s3.profile")).thenReturn("profile");

    BulkLoaderURLStreamHandlerFactory.install();
    BulkLoaderURLStreamHandlerFactory.setConfig(config);
    BulkLoaderURLStreamHandlerFactory factory = BulkLoaderURLStreamHandlerFactory.INSTANCE;

    assertThat(factory.createURLStreamHandler("std"))
        .isNotNull()
        .isInstanceOf(StdinStdoutURLStreamHandler.class);
    assertThat(factory.createURLStreamHandler("STD"))
        .isNotNull()
        .isInstanceOf(StdinStdoutURLStreamHandler.class);
    assertThat(factory.createURLStreamHandler("s3"))
        .isNotNull()
        .isInstanceOf(S3URLStreamHandler.class);
  }
}
