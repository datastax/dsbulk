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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.dsbulk.url.S3URLStreamHandler.S3Connection;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3URLStreamHandlerTest {

  @Mock private Config config;
  @Mock private InputStream mockInputStream;

  private AutoCloseable mocks;

  @BeforeEach
  void setup() {
    mocks = MockitoAnnotations.openMocks(this);

    when(config.hasPath("dsbulk.s3.clientCacheSize")).thenReturn(true);
    when(config.getInt("dsbulk.s3.clientCacheSize")).thenReturn(2);

    BulkLoaderURLStreamHandlerFactory.install();
    BulkLoaderURLStreamHandlerFactory.setConfig(config);
    // Now, calling url.openConnection() will use the right handler.
  }

  @AfterEach
  void clean_up() throws Exception {
    mocks.close();
  }

  @ParameterizedTest
  @CsvSource({
    "s3://test-bucket/test-key,You must provide S3 client credentials in the URL query parameters.",
    "s3://test-bucket/test-key?profile=default,You must supply an AWS 'region' parameter on S3 URls.",
    "s3://test-bucket/test-key?region=us-west-1&accessKeyId=secret,Both 'accessKeyId' and 'secretAccessKey' must be present if either one is provided.",
    "s3://test-bucket/test-key?region=us-west-1&secretAccessKey=secret,Both 'accessKeyId' and 'secretAccessKey' must be present if either one is provided."
  })
  void should_require_query_parameters(String s3Url, String errorMessage) throws IOException {
    URL url = new URL(s3Url);
    S3Connection connection = spy((S3Connection) url.openConnection());

    doReturn(mockInputStream).when(connection).getInputStream(any(), any());

    Throwable t = catchThrowable(connection::getInputStream);

    assertThat(t).isNotNull().isInstanceOf(IllegalArgumentException.class).hasMessage(errorMessage);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // This one uses a profile.
        "s3://test-bucket/test-key?region=us-west-1&profile=nameNoOneWouldActuallyGiveTheirProfile",
        // This one uses the default credentials provider.
        "s3://test-bucket/test-key?region=us-west-1",
        // This one uses both secret parameters.
        "s3://test-bucket/test-key?region=us-west-1&accessKeyId=secret&secretAccessKey=alsoSecret"
      })
  void should_provide_input_stream_when_parameters_are_correct(String s3Url) throws IOException {
    URL url = new URL(s3Url);
    S3Connection connection = spy((S3Connection) url.openConnection());

    doReturn(mockInputStream).when(connection).getInputStream(any(), any());

    assertThat(connection.getInputStream()).isNotNull();
  }

  @Test
  void should_cache_clients() throws IOException {
    URL url1 = new URL("s3://test-bucket/test-key-1?region=us-west-1&test=should_cache");
    S3Connection connection1 = spy((S3Connection) url1.openConnection());
    URL url2 = new URL("s3://test-bucket/test-key-2?region=us-west-1&test=should_cache");
    S3Connection connection2 = spy((S3Connection) url2.openConnection());

    S3Client mockClient = mock(S3Client.class);
    when(mockClient.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenAnswer(
            (Answer<ResponseBytes<GetObjectResponse>>)
                invocation -> {
                  GetObjectResponse response = GetObjectResponse.builder().build();
                  byte[] bytes = new byte[] {};
                  InputStream is = new ByteArrayInputStream(bytes);
                  return ResponseBytes.fromInputStream(response, is);
                });
    doReturn(mockClient).when(connection1).getS3Client(any());

    InputStream stream1 = connection1.getInputStream();
    InputStream stream2 = connection2.getInputStream();

    assertThat(stream1).isNotSameAs(stream2); // Two different URls produce different streams.
    verify(mockClient, times(2)).getObjectAsBytes(any(GetObjectRequest.class));
    verify(connection1)
        .getS3Client(
            "region=us-west-1&test=should_cache"); // We got the client for one connection...
    verify(connection2, never()).getS3Client(any()); // ... but not the second connection.
  }

  @Test
  void should_evict_cached_clients() throws IOException {
    URL url1 = new URL("s3://test-bucket/test-key-1?region=us-west-1&test=should_evict");
    S3Connection connection1 = spy((S3Connection) url1.openConnection());
    URL url2 = new URL("s3://test-bucket/test-key-2?region=us-west-2&test=should_evict");
    S3Connection connection2 = spy((S3Connection) url2.openConnection());
    URL url3 = new URL("s3://test-bucket/test-key-3?region=us-east-1&test=should_evict");
    S3Connection connection3 = spy((S3Connection) url3.openConnection());
    URL url4 = new URL("s3://test-bucket/test-key-4?region=us-west-1&test=should_evict");
    S3Connection connection4 = spy((S3Connection) url4.openConnection());

    S3Client mockClient = mock(S3Client.class);
    when(mockClient.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenAnswer(
            (Answer<ResponseBytes<GetObjectResponse>>)
                invocation -> {
                  GetObjectResponse response = GetObjectResponse.builder().build();
                  byte[] bytes = new byte[] {};
                  InputStream is = new ByteArrayInputStream(bytes);
                  return ResponseBytes.fromInputStream(response, is);
                });
    doReturn(mockClient).when(connection1).getS3Client(any());
    doReturn(mockClient).when(connection2).getS3Client(any());
    doReturn(mockClient).when(connection3).getS3Client(any());
    doReturn(mockClient).when(connection4).getS3Client(any());

    InputStream stream1 = connection1.getInputStream();
    InputStream stream2 = connection2.getInputStream();
    InputStream stream3 = connection3.getInputStream();
    InputStream stream4 = connection4.getInputStream();

    assertThat(stream1).isNotSameAs(stream2).isNotSameAs(stream3).isNotSameAs(stream4);
    assertThat(stream2).isNotSameAs(stream3).isNotSameAs(stream4);
    assertThat(stream3).isNotSameAs(stream4);
    verify(mockClient, times(4)).getObjectAsBytes(any(GetObjectRequest.class));
    verify(connection1).getS3Client("region=us-west-1&test=should_evict");
    verify(connection2).getS3Client("region=us-west-2&test=should_evict");
    verify(connection3).getS3Client("region=us-east-1&test=should_evict");
    verify(connection4)
        .getS3Client("region=us-west-1&test=should_evict"); // Original value was evicted!
  }

  @Test
  void should_not_support_writing_to_s3() throws IOException {
    URL url = new URL("s3://test-bucket/test-key");
    S3Connection connection = spy((S3Connection) url.openConnection());

    Throwable t = catchThrowable(connection::getOutputStream);

    assertThat(t)
        .isNotNull()
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Writing to S3 has not yet been implemented.");
  }
}
