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

public class S3URLStreamHandlerProvider implements URLStreamHandlerProvider {

  private static final String S3CLIENT_CACHE_SIZE_PATH = "dsbulk.s3.clientCacheSize";
  private static final int DEFAULT_S3CLIENT_CACHE_SIZE = 20;

  /** The protocol for AWS S3 URLs. I.e., URLs beginning with {@code s3://} */
  public static final String S3_STREAM_PROTOCOL = "s3";

  @Override
  @NonNull
  public Optional<URLStreamHandler> maybeCreateURLStreamHandler(
      @NonNull String protocol, Config config) {
    if (S3_STREAM_PROTOCOL.equalsIgnoreCase(protocol)) {
      int s3ClientCacheSize =
          config.hasPath(S3CLIENT_CACHE_SIZE_PATH)
              ? config.getInt(S3CLIENT_CACHE_SIZE_PATH)
              : DEFAULT_S3CLIENT_CACHE_SIZE;
      return Optional.of(new S3URLStreamHandler(s3ClientCacheSize));
    }
    return Optional.empty();
  }
}
