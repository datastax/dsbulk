/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class LineEndingDetector {

  private static final String CR = "\r";
  private static final String LF = "\n";

  private final InputStream is;

  private final byte[] crlf;
  private final byte[] lf;

  private int crlfi = 0;
  private int lfi = 0;

  private boolean iscrlf = true;
  private boolean islf = true;

  private long pos = 0L;

  public LineEndingDetector(InputStream is, Charset charset) throws FileNotFoundException {
    this.is = is;
    ByteBuffer crlfbb = charset.encode(CR + LF);
    crlf = new byte[crlfbb.remaining()];
    crlfbb.get(crlf);
    ByteBuffer lfbb = charset.encode(LF);
    lf = new byte[lfbb.remaining()];
    lfbb.get(lf);
  }

  public long nextStartOfLine() throws IOException {
    if (pos == -1) {
      return -1;
    }
    int b;
    while (true) {
      b = is.read();
      pos++;
      if (b == -1) {
        pos = -1;
        return pos;
      }
      if (iscrlf && crlf[crlfi] == b) {
        crlfi++;
        if (crlfi == crlf.length) {
          crlfi = 0;
          islf = false;
          return pos;
        }
      } else {
        crlfi = 0;
      }
      if (islf && lf[lfi] == b) {
        lfi++;
        if (lfi == lf.length) {
          lfi = 0;
          iscrlf = false;
          return pos;
        }
      } else {
        lfi = 0;
      }
    }
  }
}
