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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URLStreamHandler;
import java.util.Optional;

public class StdinStdoutURLStreamHandlerProvider implements URLStreamHandlerProvider {

  /**
   * The protocol for standard input and standard output URLs. The only supported URL with such
   * scheme is {@code std:/}.
   */
  public static final String STANDARD_STREAM_PROTOCOL = "std";

  @Override
  @NonNull
  public Optional<URLStreamHandler> maybeCreateURLStreamHandler(@NonNull String protocol) {
    if (STANDARD_STREAM_PROTOCOL.equalsIgnoreCase(protocol)) {
      return Optional.of(new StdinStdoutURLStreamHandler());
    }
    return Optional.empty();
  }
}
