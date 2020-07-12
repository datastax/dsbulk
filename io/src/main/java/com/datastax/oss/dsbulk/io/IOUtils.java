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
package com.datastax.oss.dsbulk.io;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public final class IOUtils {

  private static final int BUFFER_SIZE = 8192 * 2;

  public static BufferedInputStream newBufferedInputStream(URL url) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream
        ? (BufferedInputStream) in
        : new BufferedInputStream(in, BUFFER_SIZE);
  }

  public static BufferedOutputStream newBufferedOutputStream(URL url) throws IOException {
    OutputStream out;
    // file URLs do not support writing, only reading,
    // so we need to special-case them here
    if (url.getProtocol().equals("file")) {
      try {
        out = Files.newOutputStream(Paths.get(url.toURI()), CREATE_NEW);
      } catch (URISyntaxException e) {
        // should not happen, URLs have been validated already
        throw new IllegalArgumentException(e);
      }
    } else if (url.getProtocol().startsWith("http")) {
      throw new IllegalArgumentException("HTTP/HTTPS protocols cannot be used for output: " + url);
    } else {
      URLConnection connection = url.openConnection();
      connection.setDoOutput(true);
      out = connection.getOutputStream();
    }
    return out instanceof BufferedOutputStream
        ? (BufferedOutputStream) out
        : new BufferedOutputStream(out, BUFFER_SIZE);
  }

  public static BufferedReader newBufferedReader(URL url, Charset charset) throws IOException {
    return new BufferedReader(
        new InputStreamReader(newBufferedInputStream(url), charset), BUFFER_SIZE);
  }

  public static BufferedWriter newBufferedWriter(URL url, Charset charset) throws IOException {
    return new BufferedWriter(
        new OutputStreamWriter(newBufferedOutputStream(url), charset), BUFFER_SIZE);
  }

  public static boolean isDirectoryNonEmpty(Path path) {
    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(path)) {
      return dirStream.iterator().hasNext();
    } catch (Exception exception) {
      return true;
    }
  }

  public static long countReadableFiles(Path root, boolean recursive) throws IOException {
    try (Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1)) {
      return files.filter(Files::isReadable).filter(Files::isRegularFile).count();
    }
  }

  public static void assertAccessibleFile(Path filePath, String descriptor) {
    if (!Files.exists(filePath)) {
      throw new IllegalArgumentException(
          String.format("%s %s does not exist", descriptor, filePath));
    }
    if (!Files.isRegularFile(filePath)) {
      throw new IllegalArgumentException(
          String.format("%s %s is not a file", descriptor, filePath));
    }
    if (!Files.isReadable(filePath)) {
      throw new IllegalArgumentException(
          String.format("%s %s is not readable", descriptor, filePath));
    }
  }
}
