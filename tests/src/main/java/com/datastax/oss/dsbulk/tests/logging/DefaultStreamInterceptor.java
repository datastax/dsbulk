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
package com.datastax.oss.dsbulk.tests.logging;

import com.datastax.oss.dsbulk.url.BulkLoaderURLStreamHandlerFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import org.fusesource.jansi.AnsiString;

public class DefaultStreamInterceptor implements StreamInterceptor {

  static {
    BulkLoaderURLStreamHandlerFactory.install();
  }

  private final StreamType streamType;
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  private PrintStream originalStream;
  private boolean started = false;

  DefaultStreamInterceptor(StreamType streamType) {
    this.streamType = streamType;
  }

  @Override
  public String getStreamAsString(Charset charset) {
    return new String(baos.toByteArray(), charset);
  }

  @Override
  public String getStreamAsStringPlain(Charset charset) {
    return new AnsiString(getStreamAsString()).getPlain().toString();
  }

  @Override
  public void clear() {
    baos.reset();
  }

  void start() {
    if (!started) {
      switch (streamType) {
        case STDOUT:
          originalStream = System.out;
          System.setOut(new PrintStream(baos));
          break;
        case STDERR:
          originalStream = System.err;
          System.setErr(new PrintStream(baos));
          break;
      }
    }
    started = true;
  }

  void stop() {
    if (started) {
      switch (streamType) {
        case STDOUT:
          System.setOut(originalStream);
          break;
        case STDERR:
          System.setErr(originalStream);
          break;
      }
      clear();
      started = false;
    }
  }
}
