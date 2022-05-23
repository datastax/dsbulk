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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLStreamHandler;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  private final Cache<S3ClientInfo, S3Client> s3ClientCache;

  S3URLStreamHandler(int s3ClientCacheSize) {
    this.s3ClientCache = Caffeine.newBuilder().maximumSize(s3ClientCacheSize).build();
  }

  @Override
  protected URLConnection openConnection(URL url) {
    return new S3Connection(url, s3ClientCache);
  }

  @VisibleForTesting
  static class S3Connection extends URLConnection {

    private final Cache<S3ClientInfo, S3Client> s3ClientCache;

    @Override
    public void connect() {
      // Nothing to see here...
    }

    S3Connection(URL url, Cache<S3ClientInfo, S3Client> s3ClientCache) {
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
      if (StringUtils.isBlank(query)) {
        throw new IllegalArgumentException(
            "You must provide S3 client credentials in the URL query parameters.");
      }
      S3ClientInfo s3ClientInfo = new S3ClientInfo(query);
      S3Client s3Client = s3ClientCache.get(s3ClientInfo, this::getS3Client);
      return getInputStream(s3Client, getObjectRequest);
    }

    @VisibleForTesting
    InputStream getInputStream(S3Client s3Client, GetObjectRequest getObjectRequest) {
      return s3Client.getObjectAsBytes(getObjectRequest).asInputStream();
    }

    @VisibleForTesting
    S3Client getS3Client(S3ClientInfo s3ClientInfo) {
      SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
      S3ClientBuilder builder =
          S3Client.builder().httpClient(httpClient).region(Region.of(s3ClientInfo.getRegion()));

      String profile = s3ClientInfo.getProfile();
      String accessKeyId = s3ClientInfo.getAccessKeyId();
      String secretAccessKey = s3ClientInfo.getSecretAccessKey();
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

    @Override
    public OutputStream getOutputStream() {
      throw new UnsupportedOperationException("Writing to S3 has not yet been implemented.");
    }
  }

  @VisibleForTesting
  static class S3ClientInfo {

    private final String region;
    private final String profile;
    private final String accessKeyId;
    private final String secretAccessKey;

    S3ClientInfo(String query) {
      Map<String, List<String>> parameters;
      try {
        parameters = parseParameters(query);
      } catch (UnsupportedEncodingException e) {
        // This should never happen, since everyone should support UTF-8.
        throw new IllegalArgumentException("UTF-8 encoding was not found on your system.", e);
      }
      region = getQueryParam(parameters, REGION);
      if (StringUtils.isBlank(region)) {
        throw new IllegalArgumentException("You must supply an AWS 'region' parameter on S3 URls.");
      }
      profile = getQueryParam(parameters, PROFILE);
      accessKeyId = getQueryParam(parameters, ACCESS_KEY_ID);
      secretAccessKey = getQueryParam(parameters, SECRET_ACCESS_KEY);
    }

    // Borrowed from
    // https://stackoverflow.com/questions/13592236/parse-a-uri-string-into-name-value-collection
    private static Map<String, List<String>> parseParameters(String query)
        throws UnsupportedEncodingException {
      final Map<String, List<String>> queryPairs = new LinkedHashMap<>();
      final String[] pairs = query.split("&", 0);
      for (String pair : pairs) {
        final int idx = pair.indexOf("=");
        final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
        queryPairs.computeIfAbsent(key, k -> new ArrayList<>());
        final String value =
            idx > 0 && pair.length() > idx + 1
                ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
                : null;
        queryPairs.get(key).add(value);
      }
      return queryPairs;
    }

    private static String getQueryParam(Map<String, List<String>> parameters, String key) {
      if (parameters.containsKey(key)) {
        List<String> values = parameters.get(key);
        if (values != null && !values.isEmpty()) {
          return values.get(0);
        }
      }
      return null;
    }

    public String getRegion() {
      return region;
    }

    public String getProfile() {
      return profile;
    }

    public String getAccessKeyId() {
      return accessKeyId;
    }

    public String getSecretAccessKey() {
      return secretAccessKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      S3ClientInfo that = (S3ClientInfo) o;
      return region.equals(that.region)
          && Objects.equals(profile, that.profile)
          && Objects.equals(accessKeyId, that.accessKeyId)
          && Objects.equals(secretAccessKey, that.secretAccessKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(region, profile, accessKeyId, secretAccessKey);
    }
  }
}
