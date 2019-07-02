/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.io;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2Utils;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.deflate64.Deflate64CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMAUtils;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZUtils;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

public final class IOUtils {

  private static final int BUFFER_SIZE = 8192 * 2;
  public static final String NONE_COMPRESSION = "none";
  private static final String AUTO_COMPRESSION = "auto";
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
  private static final String ZSTD_FILE_EXTENSION = ".zstd";
  private static final String SNAPPY_FILE_EXTENSION = ".snappy";
  private static final String LZ4_FILE_EXTENSION = ".lz4";
  private static final String BROTLI_FILE_EXTENSION = ".br";
  private static final String DEFLATE_FILE_EXTENSION = ".deflate";
  private static final String Z_FILE_EXTENSION = ".z";

  // we may have different supported compressions for input & output
  private static final Map<String, Class> OUTPUT_COMPRESSORS =
      new HashMap<String, Class>() {
        {
          put(XZ_COMPRESSION, XZCompressorOutputStream.class);
          put(GZIP_COMPRESSION, GzipCompressorOutputStream.class);
          put(ZSTD_COMPRESSION, ZstdCompressorOutputStream.class);
          put(BZIP2_COMPRESSION, BZip2CompressorOutputStream.class);
          put(SNAPPY_COMPRESSION, FramedSnappyCompressorOutputStream.class);
          put(LZ4_COMPRESSION, FramedLZ4CompressorOutputStream.class);
          put(LZMA_COMPRESSION, LZMACompressorOutputStream.class);
          put(DEFLATE_COMPRESSION, DeflateCompressorOutputStream.class);
        }
      };

  // TODO: add more input compressors, like, Z, ...
  private static final Map<String, Class> INPUT_COMPRESSORS =
      new HashMap<String, Class>() {
        {
          put(XZ_COMPRESSION, XZCompressorInputStream.class);
          put(GZIP_COMPRESSION, GzipCompressorInputStream.class);
          put(ZSTD_COMPRESSION, ZstdCompressorInputStream.class);
          put(BZIP2_COMPRESSION, BZip2CompressorInputStream.class);
          put(SNAPPY_COMPRESSION, FramedSnappyCompressorInputStream.class);
          put(LZ4_COMPRESSION, FramedLZ4CompressorInputStream.class);
          put(LZMA_COMPRESSION, LZMACompressorInputStream.class);
          put(BROTLI_COMPRESSION, BrotliCompressorInputStream.class);
          put(DEFLATE_COMPRESSION, DeflateCompressorOutputStream.class);
          put(DEFLATE64_COMPRESSION, Deflate64CompressorInputStream.class);
          put(Z_COMPRESSION, ZCompressorInputStream.class);
        }
      };

  private static final Map<String, Supplier<String>> COMPRESSION_EXTENSIONS =
      new HashMap<String, Supplier<String>>() {
        {
          put(XZ_COMPRESSION, () -> XZUtils.getCompressedFilename(""));
          put(GZIP_COMPRESSION, () -> GzipUtils.getCompressedFilename(""));
          put(ZSTD_COMPRESSION, () -> ZSTD_FILE_EXTENSION);
          put(BZIP2_COMPRESSION, () -> BZip2Utils.getCompressedFilename(""));
          put(SNAPPY_COMPRESSION, () -> SNAPPY_FILE_EXTENSION);
          put(LZ4_COMPRESSION, () -> LZ4_FILE_EXTENSION);
          put(LZMA_COMPRESSION, () -> LZMAUtils.getCompressedFilename(""));
          put(DEFLATE_COMPRESSION, () -> DEFLATE_FILE_EXTENSION);
        }
      };

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

  public static LineNumberReader newBufferedReader(URL url, Charset charset) throws IOException {
    return new LineNumberReader(
        new InputStreamReader(newBufferedInputStream(url), charset), BUFFER_SIZE);
  }

  public static LineNumberReader newBufferedReader(
      final URL url, final Charset charset, final String compression) throws IOException {
    final LineNumberReader reader;
    if (compression == null || compression.equalsIgnoreCase(NONE_COMPRESSION))
      reader = newBufferedReader(url, charset);
    else {
      String compMethod = compression;
      if (isAutoCompression(compression)) {
        compMethod = detectCompression(url.toString(), true);
      }
      Class compressorClass = INPUT_COMPRESSORS.get(compMethod.toLowerCase());
      if (compressorClass == null) {
        throw new IOException("Unsupported compression format: " + compMethod);
      }
      InputStream in = newBufferedInputStream(url);
      try {
        CompressorInputStream cin =
            (CompressorInputStream)
                compressorClass.getDeclaredConstructor(InputStream.class).newInstance(in);
        reader = new LineNumberReader(new InputStreamReader(cin, charset), BUFFER_SIZE);
      } catch (NoSuchMethodException
          | IllegalAccessException
          | InstantiationException
          | InvocationTargetException ex) {
        // ex.printStackTrace();
        throw new IOException("Can't instantiate class for compression: " + compression, ex);
      }
    }
    return reader;
  }

  public static BufferedWriter newBufferedWriter(URL url, Charset charset) throws IOException {
    return new BufferedWriter(
        new OutputStreamWriter(newBufferedOutputStream(url), charset), BUFFER_SIZE);
  }

  public static BufferedWriter newBufferedWriter(
      final URL url, final Charset charset, final String compression) throws IOException {
    final BufferedWriter writer;
    if (compression == null || compression.equalsIgnoreCase(NONE_COMPRESSION))
      writer = newBufferedWriter(url, charset);
    else {
      Class compressorClass = OUTPUT_COMPRESSORS.get(compression.toLowerCase());
      if (compressorClass == null) {
        throw new IOException("Unsupported compression format: " + compression);
      }
      OutputStream os = newBufferedOutputStream(url);
      try {
        CompressorOutputStream cos =
            (CompressorOutputStream)
                compressorClass.getDeclaredConstructor(OutputStream.class).newInstance(os);
        writer = new BufferedWriter(new OutputStreamWriter(cos, charset), BUFFER_SIZE);
      } catch (NoSuchMethodException
          | IllegalAccessException
          | InstantiationException
          | InvocationTargetException ex) {
        // ex.printStackTrace();
        throw new IOException("Can't instantiate class for compression: " + compression, ex);
      }
    }
    return writer;
  }

  public static String getCompressionSuffix(final String compression) {
    if (compression == null || compression.equalsIgnoreCase(NONE_COMPRESSION)) {
      return "";
    }
    return COMPRESSION_EXTENSIONS.get(compression).get();
  }

  public static List<String> getSupportedCompressions(boolean isRead) {
    List<String> lst = new ArrayList<>();
    lst.add("none");
    lst.add("auto");
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

  public static String detectCompression(final String url, boolean isRead) {
    String name = url;
    if (name.endsWith(File.separator)) {
      name = name.substring(0, name.length() - 1);
    }
    name = name.toLowerCase();
    if (XZUtils.isCompressedFilename(name)) {
      return XZ_COMPRESSION;
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
    } else if (isRead && name.endsWith(Z_FILE_EXTENSION)) {
      return Z_COMPRESSION;
    }

    return NONE_COMPRESSION;
  }

  public static boolean isAutoCompression(final String compression) {
    return AUTO_COMPRESSION.equalsIgnoreCase(compression);
  }

  public static boolean isNoneCompression(final String compression) {
    return NONE_COMPRESSION.equalsIgnoreCase(compression);
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
}
