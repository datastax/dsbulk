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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class S3URLStreamHandlerProviderTest {

  @Test
  void should_handle_s3_protocol() {
    Config config = mock(Config.class);
    when(config.hasPath("dsbulk.s3.region")).thenReturn(true);
    when(config.getString("dsbulk.s3.region")).thenReturn("us-west-1");
    when(config.hasPath("dsbulk.s3.profile")).thenReturn(true);
    when(config.getString("dsbulk.s3.profile")).thenReturn("profile");
    when(config.hasPath("dsbulk.s3.accessKeyId")).thenReturn(true);
    when(config.getString("dsbulk.s3.accessKeyId")).thenReturn("accessKeyId");
    when(config.hasPath("dsbulk.s3.secretAccessKey")).thenReturn(true);
    when(config.getString("dsbulk.s3.secretAccessKey")).thenReturn("secretAccessKey");

    S3URLStreamHandlerProvider provider = new S3URLStreamHandlerProvider();

    assertThat(provider.maybeCreateURLStreamHandler("s3", config))
        .isNotNull()
        .containsInstanceOf(S3URLStreamHandler.class);
  }

  @Test
  void should_require_region_parameter() {
    Config config = mock(Config.class);
    when(config.hasPath("dsbulk.s3.region")).thenReturn(false);

    S3URLStreamHandlerProvider provider = new S3URLStreamHandlerProvider();

    Throwable t = catchThrowable(() -> provider.maybeCreateURLStreamHandler("s3", config));

    assertThat(t)
        .isNotNull()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("You must supply an AWS region to use S3 URls.");
  }
}
