/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.io;

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
import java.nio.file.Files;
import java.nio.file.Paths;

/** */
public final class IOUtils {

  private static final int BUFFER_SIZE = 8192 * 2;

  public static BufferedInputStream newBufferedInputStream(URL url) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream
        ? (BufferedInputStream) in
        : new BufferedInputStream(in, BUFFER_SIZE);
  }

  public static BufferedOutputStream newBufferedOutputStream(URL url)
      throws IOException, URISyntaxException {
    OutputStream out;
    // file URLs do not support writing, only reading,
    // so we need to special-case them here
    if (url.getProtocol().equals("file")) {
      out = Files.newOutputStream(Paths.get(url.toURI()), CREATE_NEW);
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

  public static BufferedWriter newBufferedWriter(URL url, Charset charset)
      throws IOException, URISyntaxException {
    return new BufferedWriter(
        new OutputStreamWriter(newBufferedOutputStream(url), charset), BUFFER_SIZE);
  }
}
