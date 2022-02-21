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

import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URLStreamHandler;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.StringUtils;

public class S3URLStreamHandlerProvider implements URLStreamHandlerProvider {

  private static final String REGION_PATH = "dsbulk.s3.region";
  private static final String PROFILE_PATH = "dsbulk.s3.profile";

  private static final Logger LOGGER = LoggerFactory.getLogger(S3URLStreamHandlerProvider.class);

  /** The protocol for AWS S3 URLs. I.e., URLs beginning with {@code s3://} */
  public static final String S3_STREAM_PROTOCOL = "s3";

  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private S3Client s3Client;

  private void init(Config config) {
    if (INITIALIZED.get()) {
      LOGGER.warn("Ignoring additional attempts to initialize the S3URLStreamHandlerProvider.");
      return;
    }

    String region;
    if (config.hasPath(REGION_PATH)) {
      region = config.getString(REGION_PATH);
    } else {
      throw new IllegalArgumentException("You must supply an AWS region to use S3 URls.");
    }
    String profile = config.hasPath(PROFILE_PATH) ? config.getString(PROFILE_PATH) : null;

    SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
    S3ClientBuilder builder = S3Client.builder().httpClient(httpClient).region(Region.of(region));

    if (!StringUtils.isBlank(profile)) {
      LOGGER.info("Using AWS profile {} to connect to S3.", profile);
      builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
    } else {
      LOGGER.info("Using default credentials provider to connect to S3.");
    }

    this.s3Client = builder.build();
  }

  @Override
  @NonNull
  public Optional<URLStreamHandler> maybeCreateURLStreamHandler(
      @NonNull String protocol, Config config) {
    if (S3_STREAM_PROTOCOL.equalsIgnoreCase(protocol)) {
      init(config);
      return Optional.of(new S3URLStreamHandler(s3Client));
    }
    return Optional.empty();
  }
}
