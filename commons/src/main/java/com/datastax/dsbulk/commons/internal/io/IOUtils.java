/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.io;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;

/** */
public final class IOUtils {

  public static InputStream openInputStream(URL url) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream ? in : new BufferedInputStream(in);
  }

  public static OutputStream openOutputStream(URL url) throws IOException, URISyntaxException {
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
    return out instanceof BufferedOutputStream ? out : new BufferedOutputStream(out);
  }
}
