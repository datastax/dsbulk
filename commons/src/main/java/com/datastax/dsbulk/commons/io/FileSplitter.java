/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.io;

import com.datastax.dsbulk.commons.url.URLUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileSplitter {

  private final URL url;
  private final Charset encoding;
  private final boolean header;
  private final long eof;
  private long skipLines;
  private long chunkSize;

  public FileSplitter(File file, Charset encoding, boolean header, long skipLines, long chunkSize)
      throws MalformedURLException {
    this.url = file.toURI().toURL();
    this.eof = file.length();
    this.encoding = encoding;
    this.header = header;
    this.skipLines = skipLines;
    this.chunkSize = chunkSize;
  }

  public List<URL> split() throws IOException {
    List<URL> chunks = new ArrayList<>();
    try (InputStream is = URLUtils.openInputStream(url)) {
      LineEndingDetector detector = new LineEndingDetector(is, encoding);
      long start = 0L;
      long secondLineStart = -1L;
      if (header) {
        secondLineStart = detector.nextStartOfLine();
        start = secondLineStart;
      }
      if (skipLines > 0) {
        for (int i = 0; i < skipLines; i++) {
          start = detector.nextStartOfLine();
        }
      }
      if (start == -1L || start == eof) {
        // nothing left to read
        return Collections.emptyList();
      }
      if (header) {
        // since all chunks will have to read the header line,
        // adjust the chunk size
        chunkSize -= secondLineStart;
      }
      long end = -1L;
      long limit = Math.min(start + chunkSize, eof);
      long nextStart;
      while (true) {
        nextStart = detector.nextStartOfLine();
        if (nextStart == -1L) {
          // we reached end of file
          nextStart = eof;
        }
        if (nextStart > limit) {
          if (end == -1L) {
            // the chunk size wasn't large enough to contain the next line,
            // we need to extend it temporarily (shouldn't happen in real life)
            end = nextStart;
          }
          chunks.add(newChunk(start, end, secondLineStart));
          start = end;
          end = -1L;
          limit = Math.min(start + chunkSize, eof);
        } else {
          end = nextStart;
        }
        if (nextStart == eof) {
          if (start != eof) {
            // there is still some last lines left
            URL chunk = newChunk(start, eof, secondLineStart);
            chunks.add(chunk);
          }
          break;
        }
      }
      return chunks;
    }
  }

  private URL newChunk(long start, long end, long secondLineStart) throws MalformedURLException {
    URL chunk;
    if (header) {
      chunk =
          new URL(
              String.format(
                  "mapped-" + url.toExternalForm() + "?range=0-%d&range=%d-%d",
                  secondLineStart,
                  start,
                  end));
    } else {
      chunk = new URL(String.format("mapped-" + url.toExternalForm() + "?range=%d-%d", start, end));
    }
    return chunk;
  }
}
