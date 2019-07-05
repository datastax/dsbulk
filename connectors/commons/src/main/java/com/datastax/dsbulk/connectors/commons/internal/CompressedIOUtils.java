/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.commons.internal;

import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2Utils;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.compress.compressors.lzma.LZMAUtils;
import org.apache.commons.compress.compressors.xz.XZUtils;

public final class CompressedIOUtils {

  private static final int BUFFER_SIZE = 8192 * 2;
  public static final String NONE_COMPRESSION = "none";
  public static final String AUTO_COMPRESSION = "auto";
  public static final String XZ_COMPRESSION = "xz";
  public static final String GZIP_COMPRESSION = "gzip";
  public static final String ZSTD_COMPRESSION = "zstd";
  public static final String BZIP2_COMPRESSION = "bzip2";
  public static final String SNAPPY_COMPRESSION = "snappy";
  public static final String LZ4_COMPRESSION = "lz4";
  public static final String LZMA_COMPRESSION = "lzma";
  public static final String BROTLI_COMPRESSION = "brotli";
  public static final String DEFLATE_COMPRESSION = "deflate";
  public static final String DEFLATE64_COMPRESSION = "deflate64";
  public static final String Z_COMPRESSION = "z";
  @VisibleForTesting private static final String ZSTD_FILE_EXTENSION = ".zstd";
  @VisibleForTesting private static final String SNAPPY_FILE_EXTENSION = ".snappy";
  @VisibleForTesting private static final String LZ4_FILE_EXTENSION = ".lz4";
  @VisibleForTesting private static final String BROTLI_FILE_EXTENSION = ".br";
  @VisibleForTesting private static final String DEFLATE_FILE_EXTENSION = ".deflate";
  @VisibleForTesting private static final String Z_FILE_EXTENSION = ".z";

  // we may have different supported compressions for input & output
  private static final ImmutableMap<String, String> OUTPUT_COMPRESSORS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, CompressorStreamFactory.XZ)
          .put(GZIP_COMPRESSION, CompressorStreamFactory.GZIP)
          .put(ZSTD_COMPRESSION, CompressorStreamFactory.ZSTANDARD)
          .put(BZIP2_COMPRESSION, CompressorStreamFactory.BZIP2)
          .put(SNAPPY_COMPRESSION, CompressorStreamFactory.SNAPPY_FRAMED)
          .put(LZ4_COMPRESSION, CompressorStreamFactory.LZ4_FRAMED)
          .put(LZMA_COMPRESSION, CompressorStreamFactory.LZMA)
          .put(DEFLATE_COMPRESSION, CompressorStreamFactory.DEFLATE)
          .build();

  private static final ImmutableMap<String, String> INPUT_COMPRESSORS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, CompressorStreamFactory.XZ)
          .put(GZIP_COMPRESSION, CompressorStreamFactory.GZIP)
          .put(ZSTD_COMPRESSION, CompressorStreamFactory.ZSTANDARD)
          .put(BZIP2_COMPRESSION, CompressorStreamFactory.BZIP2)
          .put(SNAPPY_COMPRESSION, CompressorStreamFactory.SNAPPY_FRAMED)
          .put(LZ4_COMPRESSION, CompressorStreamFactory.LZ4_FRAMED)
          .put(LZMA_COMPRESSION, CompressorStreamFactory.LZMA)
          .put(BROTLI_COMPRESSION, CompressorStreamFactory.BROTLI)
          .put(DEFLATE_COMPRESSION, CompressorStreamFactory.DEFLATE)
          .put(DEFLATE64_COMPRESSION, CompressorStreamFactory.DEFLATE64)
          .put(Z_COMPRESSION, CompressorStreamFactory.Z)
          .build();

  private static final ImmutableMap<String, String> COMPRESSION_EXTENSIONS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, ".xz")
          .put(GZIP_COMPRESSION, ".gz")
          .put(ZSTD_COMPRESSION, ZSTD_FILE_EXTENSION)
          .put(BZIP2_COMPRESSION, ".bz2")
          .put(SNAPPY_COMPRESSION, SNAPPY_FILE_EXTENSION)
          .put(LZ4_COMPRESSION, LZ4_FILE_EXTENSION)
          .put(LZMA_COMPRESSION, ".lzma")
          .put(DEFLATE_COMPRESSION, DEFLATE_FILE_EXTENSION)
          .build();

  public static LineNumberReader newBufferedReader(
      final URL url, final Charset charset, final String compression) throws IOException {
    final LineNumberReader reader;
    if (compression == null || isNoneCompression(compression)) {
      reader = IOUtils.newBufferedReader(url, charset);
    } else {
      String compMethod = compression;
      if (isAutoCompression(compression)) {
        compMethod = detectCompression(url.toString(), true);
        if (isNoneCompression(compMethod)) {
          return IOUtils.newBufferedReader(url, charset);
        }
      }

      String compressor = INPUT_COMPRESSORS.get(compMethod.toLowerCase());
      if (compressor == null) {
        throw new IOException("Unsupported compression format: " + compMethod);
      }
      InputStream in = IOUtils.newBufferedInputStream(url);
      try {
        CompressorInputStream cin =
            new CompressorStreamFactory().createCompressorInputStream(compressor, in);
        reader = new LineNumberReader(new InputStreamReader(cin, charset), BUFFER_SIZE);
      } catch (CompressorException ex) {
        // ex.printStackTrace();
        throw new IOException("Can't instantiate class for compression: " + compMethod, ex);
      }
    }
    return reader;
  }

  public static BufferedWriter newBufferedWriter(
      final URL url, final Charset charset, final String compression) throws IOException {
    final BufferedWriter writer;
    if (compression == null || compression.equalsIgnoreCase(NONE_COMPRESSION))
      writer = IOUtils.newBufferedWriter(url, charset);
    else {
      String compressor = OUTPUT_COMPRESSORS.get(compression.toLowerCase());
      if (compressor == null) {
        throw new IOException("Unsupported compression format: " + compression);
      }
      OutputStream os = IOUtils.newBufferedOutputStream(url);
      try {
        CompressorOutputStream cos =
            new CompressorStreamFactory().createCompressorOutputStream(compressor, os);
        writer = new BufferedWriter(new OutputStreamWriter(cos, charset), BUFFER_SIZE);
      } catch (CompressorException ex) {
        // ex.printStackTrace();
        throw new IOException("Can't instantiate class for compression: " + compression, ex);
      }
    }
    return writer;
  }

  public static String getCompressionSuffix(final String compression) {
    return COMPRESSION_EXTENSIONS.getOrDefault(compression, "");
  }

  public static List<String> getSupportedCompressions(boolean isRead) {
    List<String> lst = new ArrayList<>();
    lst.add(AUTO_COMPRESSION);
    lst.add(NONE_COMPRESSION);
    if (isRead) {
      lst.addAll(INPUT_COMPRESSORS.keySet());
    } else {
      lst.addAll(OUTPUT_COMPRESSORS.keySet());
    }
    return lst;
  }

  public static Boolean isSupportedCompression(final String compression, boolean isRead) {
    if (compression == null) {
      return false;
    }
    if (isRead) {
      return INPUT_COMPRESSORS.containsKey(compression)
          || compression.equalsIgnoreCase(NONE_COMPRESSION)
          || compression.equalsIgnoreCase(AUTO_COMPRESSION);
    }
    return OUTPUT_COMPRESSORS.containsKey(compression)
        || compression.equalsIgnoreCase(NONE_COMPRESSION);
  }

  /**
   * Tries to detect compression type based on the file name. Only the file extension is used for
   * detection.
   *
   * @param url - URL
   * @param isRead - true if the read operation performed, as the list of compressors is different
   *     for load &amp; unload operations
   * @return type of detected compression
   */
  public static String detectCompression(final String url, boolean isRead) {
    String name = url;
    if (name.endsWith(File.separator)) {
      name = name.substring(0, name.length() - 1);
    }
    name = name.toLowerCase();
    if (XZUtils.isCompressedFilename(name)) {
      return XZ_COMPRESSION;
    } else if (isRead && name.endsWith(Z_FILE_EXTENSION)) {
      return Z_COMPRESSION;
    } else if (GzipUtils.isCompressedFilename(name)) {
      return GZIP_COMPRESSION;
    } else if (BZip2Utils.isCompressedFilename(name)) {
      return BZIP2_COMPRESSION;
    } else if (LZMAUtils.isCompressedFilename(name)) {
      return LZMA_COMPRESSION;
    } else if (name.endsWith(ZSTD_FILE_EXTENSION)) {
      return ZSTD_COMPRESSION;
    } else if (name.endsWith(SNAPPY_FILE_EXTENSION)) {
      return SNAPPY_COMPRESSION;
    } else if (name.endsWith(LZ4_FILE_EXTENSION)) {
      return LZ4_COMPRESSION;
    } else if (name.endsWith(DEFLATE_FILE_EXTENSION)) {
      return DEFLATE_COMPRESSION;
    } else if (isRead && name.endsWith(BROTLI_FILE_EXTENSION)) {
      return BROTLI_COMPRESSION;
    }

    return NONE_COMPRESSION;
  }

  public static boolean isAutoCompression(final String compression) {
    return AUTO_COMPRESSION.equalsIgnoreCase(compression);
  }

  public static boolean isNoneCompression(final String compression) {
    return NONE_COMPRESSION.equalsIgnoreCase(compression);
  }
}
