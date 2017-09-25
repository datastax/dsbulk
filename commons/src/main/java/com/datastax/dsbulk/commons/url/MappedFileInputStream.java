/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import com.google.common.collect.Range;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * A memory-mapped file input stream that recognizes URLs of the following form:
 *
 * <pre>
 * mapped-file:/path/to/my/file
 * mapped-file:/path/to/my/file?range=s1-e1&range=s2-e2
 * </pre>
 */
public class MappedFileInputStream extends InputStream {

  private final FileChannel channel;
  private final List<MappedByteBuffer> buffers;
  private int current;

  public MappedFileInputStream(URL url) throws IOException {
    if (!url.getProtocol().equalsIgnoreCase(LoaderURLStreamHandlerFactory.MAPPED_FILE)) {
      throw new IllegalArgumentException("Invalid URL protocol: " + url.getProtocol());
    }
    this.channel = new RandomAccessFile(url.getPath(), "r").getChannel();
    List<Range<Long>> ranges = new ArrayList<>();
    if (url.getQuery() != null) {
      for (String token : url.getQuery().split("&")) {
        int i = token.indexOf('=');
        if (!token.substring(0, i).equals("range")) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid query parameter, expecting 'range', got '%s'", token.substring(0, i)));
        }
        int n = token.indexOf('-');
        if (n == -1) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid query parameter value, expecting 'start-end', got '%s'",
                  token.substring(i + 1)));
        }
        long start = Long.parseLong(token.substring(i + 1, n));
        long end = Long.parseLong(token.substring(n + 1));
        Range<Long> range = Range.closedOpen(start, end);
        ranges.add(range);
      }
      buffers = new ArrayList<>(ranges.size());
      for (Range<Long> range : ranges) {
        long start = range.lowerEndpoint();
        long end = range.upperEndpoint();
        long size = end - start;
        buffers.add(channel.map(FileChannel.MapMode.READ_ONLY, start, size));
      }
    } else {
      buffers = new ArrayList<>(1);
      buffers.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()));
    }
  }

  @Override
  public int read() throws IOException {
    ByteBuffer buffer = getBuffer();
    if (buffer == null) {
      return -1;
    }
    return buffer.get() & 0xff;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    ByteBuffer buffer = getBuffer();
    if (buffer == null) {
      return -1;
    }
    int remaining = buffer.remaining();
    if (len > remaining) {
      buffer.get(b, off, remaining);
      return remaining;
    } else {
      buffer.get(b, off, len);
      return len;
    }
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private ByteBuffer getBuffer() {
    while (current < buffers.size()) {
      ByteBuffer buffer = buffers.get(current);
      if (buffer.hasRemaining()) {
        return buffer;
      }
      current++;
    }
    return null;
  }
}
