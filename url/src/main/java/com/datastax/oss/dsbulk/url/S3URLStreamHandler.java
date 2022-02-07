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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/** A {@link URLStreamHandler} for reading from AWS S3 URls. */
public class S3URLStreamHandler extends URLStreamHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3URLStreamHandler.class);

  private final S3Client s3Client;

  S3URLStreamHandler(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  protected URLConnection openConnection(URL url) {
    return new S3Connection(url, s3Client);
  }

  private static class S3Connection extends URLConnection {

    private final S3Client s3Client;

    @Override
    public void connect() {
      // Nothing to see here...
    }

    S3Connection(URL url, S3Client s3Client) {
      super(url);
      this.s3Client = s3Client;
    }

    @Override
    public InputStream getInputStream() {
      String bucket = url.getHost();
      String key = url.getPath().substring(1); // Strip leading '/'.
      LOGGER.debug("Getting S3 input stream for object '{}' in bucket '{}'...", key, bucket);
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(bucket).key(key).build();
      return s3Client.getObjectAsBytes(getObjectRequest).asInputStream();
    }

    @Override
    public OutputStream getOutputStream() {
      throw new UnsupportedOperationException("Writing to S3 has not yet been implemented.");
    }
  }
}
