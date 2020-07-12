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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * A {@link URLStreamHandler} for reading from {@link System#in} and writing to {@link System#out}.
 */
public class StdinStdoutURLStreamHandler extends URLStreamHandler {

  StdinStdoutURLStreamHandler() {}

  @Override
  protected URLConnection openConnection(URL url) {
    return new StdinStdoutConnection(url);
  }

  private static class StdinStdoutConnection extends URLConnection {

    @Override
    public void connect() {}

    StdinStdoutConnection(URL url) {
      super(url);
    }

    @Override
    public InputStream getInputStream() {
      return new BufferedInputStream(new UncloseableInputStream(System.in));
    }

    @Override
    public OutputStream getOutputStream() {
      return new BufferedOutputStream(new UncloseableOutputStream(System.out));
    }
  }
}
