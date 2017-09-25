/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

public class URLUtils {

  private static final int DEFAULT_BUFFERT_SIZE = 128 * 1024; //128K

  public static InputStream openInputStream(URL url) throws IOException {
    return openInputStream(url, DEFAULT_BUFFERT_SIZE);
  }

  public static InputStream openInputStream(URL url, int bufferSize) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream ? in : new BufferedInputStream(in, bufferSize);
  }

  public static OutputStream openOutputStream(URL url) throws IOException, URISyntaxException {
    return openOutputStream(url, DEFAULT_BUFFERT_SIZE);
  }

  public static OutputStream openOutputStream(URL url, int bufferSize)
      throws IOException, URISyntaxException {
    OutputStream out;
    // file URLs do not support writing, only reading,
    // so we need to special-case them here
    if (url.getProtocol().equals("file")) {
      out = new FileOutputStream(new File(url.toURI()));
    } else {
      URLConnection connection = url.openConnection();
      connection.setDoOutput(true);
      out = connection.getOutputStream();
    }
    return out instanceof BufferedOutputStream ? out : new BufferedOutputStream(out, bufferSize);
  }
}
