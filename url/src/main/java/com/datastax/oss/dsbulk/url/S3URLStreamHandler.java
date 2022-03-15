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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.utils.StringUtils;

/** A {@link URLStreamHandler} for reading from AWS S3 URls. */
public class S3URLStreamHandler extends URLStreamHandler {

  private static final String REGION = "region";
  private static final String PROFILE = "profile";
  private static final String ACCESS_KEY_ID = "accessKeyId";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";

  private static final Logger LOGGER = LoggerFactory.getLogger(S3URLStreamHandler.class);

  private final Map<String, S3Client> s3ClientCache;

  S3URLStreamHandler(int s3ClientCacheSize) {
    this.s3ClientCache = Collections.synchronizedMap(new LRUMap<>(s3ClientCacheSize));
  }

  @Override
  protected URLConnection openConnection(URL url) {
    return new S3Connection(url, s3ClientCache);
  }

  @VisibleForTesting
  static class S3Connection extends URLConnection {

    private final Map<String, S3Client> s3ClientCache;

    @Override
    public void connect() {
      // Nothing to see here...
    }

    S3Connection(URL url, Map<String, S3Client> s3ClientCache) {
      super(url);
      this.s3ClientCache = s3ClientCache;
    }

    @Override
    public InputStream getInputStream() {
      String bucket = url.getHost();
      String key = url.getPath().substring(1); // Strip leading '/'.
      LOGGER.debug("Getting S3 input stream for object '{}' in bucket '{}'...", key, bucket);
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(bucket).key(key).build();
      String query = url.getQuery();
      if (query == null) {
        throw new IllegalArgumentException(
            "You must provide S3 client credentials in the URL query parameters.");
      }
      S3Client s3Client = s3ClientCache.computeIfAbsent(query, this::getS3Client);
      return getInputStream(s3Client, getObjectRequest);
    }

    @VisibleForTesting
    InputStream getInputStream(S3Client s3Client, GetObjectRequest getObjectRequest) {
      return s3Client.getObjectAsBytes(getObjectRequest).asInputStream();
    }

    @VisibleForTesting
    S3Client getS3Client(String query) {
      Map<String, String> parameters = parseParameters(query);
      String region;
      if (parameters.containsKey(REGION)) {
        region = parameters.get(REGION);
      } else {
        throw new IllegalArgumentException("You must supply an AWS 'region' parameter on S3 URls.");
      }

      SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
      S3ClientBuilder builder = S3Client.builder().httpClient(httpClient).region(Region.of(region));

      String profile = parameters.getOrDefault(PROFILE, null);
      String accessKeyId = parameters.getOrDefault(ACCESS_KEY_ID, null);
      String secretAccessKey = parameters.getOrDefault(SECRET_ACCESS_KEY, null);
      if (!StringUtils.isBlank(profile)) {
        LOGGER.info("Using AWS profile {} to connect to S3.", profile);
        builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
      } else if (accessKeyId != null || secretAccessKey != null) {
        LOGGER.warn("Using access key ID and secret access key to connect to S3.");
        LOGGER.warn(
            "This is considered an insecure way to pass credentials. Please use a 'profile' instead.");
        if (accessKeyId == null || secretAccessKey == null) {
          throw new IllegalArgumentException(
              "Both 'accessKeyId' and 'secretAccessKey' must be present if either one is provided.");
        }
        builder.credentialsProvider(() -> AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        LOGGER.info("Using default credentials provider to connect to S3.");
      }

      return builder.build();
    }

    private Map<String, String> parseParameters(String query) {
      Map<String, String> parameters = new HashMap<>();
      String[] paramList = query.split("&", 0);
      for (String param : paramList) {
        String[] parts = param.split("=", 0);
        if (parts.length > 1 && !StringUtils.isBlank(parts[1])) {
          parameters.put(parts[0], parts[1]);
        }
      }
      return parameters;
    }

    @Override
    public OutputStream getOutputStream() {
      throw new UnsupportedOperationException("Writing to S3 has not yet been implemented.");
    }
  }
}
