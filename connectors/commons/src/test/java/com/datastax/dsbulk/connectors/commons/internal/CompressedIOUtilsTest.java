/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.commons.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CompresssedCompressedIOUtilsTest {

  @Test
  void getCompressionSuffix() {
    assertEquals(".gz", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.GZIP_COMPRESSION));
    assertEquals(".xz", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.XZ_COMPRESSION));
    assertEquals(
        ".bz2", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.BZIP2_COMPRESSION));
    assertEquals(
        ".lzma", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.LZMA_COMPRESSION));
    assertEquals(
        ".zstd", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.ZSTD_COMPRESSION));
    assertEquals(".lz4", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.LZ4_COMPRESSION));
    assertEquals(
        ".snappy", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.SNAPPY_COMPRESSION));
    assertEquals(
        ".deflate", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.DEFLATE_COMPRESSION));

    assertEquals("", CompressedIOUtils.getCompressionSuffix(CompressedIOUtils.AUTO_COMPRESSION));
    assertEquals("", CompressedIOUtils.getCompressionSuffix("abc"));
  }

  @Test
  void getSupportedCompressions() {
    Set<String> s = new HashSet<>(CompressedIOUtils.getSupportedCompressions(true));
    assertTrue(s.contains(CompressedIOUtils.AUTO_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.NONE_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.XZ_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.GZIP_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.ZSTD_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.BZIP2_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.SNAPPY_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.LZ4_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.LZMA_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.BROTLI_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.DEFLATE_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.DEFLATE64_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.Z_COMPRESSION));

    //
    s.clear();
    s.addAll(CompressedIOUtils.getSupportedCompressions(false));
    assertTrue(s.contains(CompressedIOUtils.AUTO_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.NONE_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.XZ_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.GZIP_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.ZSTD_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.BZIP2_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.SNAPPY_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.LZ4_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.LZMA_COMPRESSION));
    assertFalse(s.contains(CompressedIOUtils.BROTLI_COMPRESSION));
    assertTrue(s.contains(CompressedIOUtils.DEFLATE_COMPRESSION));
    assertFalse(s.contains(CompressedIOUtils.DEFLATE64_COMPRESSION));
    assertFalse(s.contains(CompressedIOUtils.Z_COMPRESSION));
  }

  @Test
  void isSupportedCompression() {
    // supported for both reads & writes
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.XZ_COMPRESSION, true));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.XZ_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.GZIP_COMPRESSION, true));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.GZIP_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.ZSTD_COMPRESSION, true));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.ZSTD_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.BZIP2_COMPRESSION, true));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.BZIP2_COMPRESSION, false));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.SNAPPY_COMPRESSION, true));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.SNAPPY_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.LZ4_COMPRESSION, true));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.LZ4_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.LZMA_COMPRESSION, true));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.LZMA_COMPRESSION, false));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.DEFLATE_COMPRESSION, true));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.DEFLATE_COMPRESSION, false));

    // supported only for reads
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.BROTLI_COMPRESSION, true));
    assertFalse(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.BROTLI_COMPRESSION, false));
    assertTrue(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.Z_COMPRESSION, true));
    assertFalse(CompressedIOUtils.isSupportedCompression(CompressedIOUtils.Z_COMPRESSION, false));
    assertTrue(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.DEFLATE64_COMPRESSION, true));
    assertFalse(
        CompressedIOUtils.isSupportedCompression(CompressedIOUtils.DEFLATE64_COMPRESSION, false));

    // unknown compressions
    assertFalse(CompressedIOUtils.isSupportedCompression(null, true));
    assertFalse(CompressedIOUtils.isSupportedCompression(null, false));
    assertFalse(CompressedIOUtils.isSupportedCompression("abc", true));
    assertFalse(CompressedIOUtils.isSupportedCompression("abc", false));
  }

  @Test
  void detectCompression() {
    // for reading
    assertEquals(
        CompressedIOUtils.GZIP_COMPRESSION, CompressedIOUtils.detectCompression("124.gz", true));
    assertEquals(
        CompressedIOUtils.ZSTD_COMPRESSION, CompressedIOUtils.detectCompression("124.zstd", true));
    assertEquals(
        CompressedIOUtils.BROTLI_COMPRESSION, CompressedIOUtils.detectCompression("124.br", true));
    assertEquals(
        CompressedIOUtils.BZIP2_COMPRESSION, CompressedIOUtils.detectCompression("124.bz2", true));
    assertEquals(
        CompressedIOUtils.DEFLATE_COMPRESSION,
        CompressedIOUtils.detectCompression("124.deflate", true));
    assertEquals(
        CompressedIOUtils.SNAPPY_COMPRESSION,
        CompressedIOUtils.detectCompression("124.snappy", true));
    assertEquals(
        CompressedIOUtils.XZ_COMPRESSION, CompressedIOUtils.detectCompression("124.xz", true));
    assertEquals(
        CompressedIOUtils.LZ4_COMPRESSION, CompressedIOUtils.detectCompression("124.lz4", true));
    assertEquals(
        CompressedIOUtils.LZMA_COMPRESSION, CompressedIOUtils.detectCompression("124.lzma", true));
    assertEquals(
        CompressedIOUtils.Z_COMPRESSION, CompressedIOUtils.detectCompression("124.z", true));
    assertEquals(
        CompressedIOUtils.NONE_COMPRESSION, CompressedIOUtils.detectCompression("124.csv", true));

    // for writing
    assertEquals(
        CompressedIOUtils.GZIP_COMPRESSION, CompressedIOUtils.detectCompression("124.gz", false));
    assertEquals(
        CompressedIOUtils.ZSTD_COMPRESSION, CompressedIOUtils.detectCompression("124.zstd", false));
    assertEquals(
        CompressedIOUtils.BZIP2_COMPRESSION, CompressedIOUtils.detectCompression("124.bz2", false));
    assertEquals(
        CompressedIOUtils.DEFLATE_COMPRESSION,
        CompressedIOUtils.detectCompression("124.deflate", false));
    assertEquals(
        CompressedIOUtils.SNAPPY_COMPRESSION,
        CompressedIOUtils.detectCompression("124.snappy", false));
    assertEquals(
        CompressedIOUtils.XZ_COMPRESSION, CompressedIOUtils.detectCompression("124.xz", false));
    assertEquals(
        CompressedIOUtils.LZ4_COMPRESSION, CompressedIOUtils.detectCompression("124.lz4", false));
    assertEquals(
        CompressedIOUtils.LZMA_COMPRESSION, CompressedIOUtils.detectCompression("124.lzma", false));
    assertEquals(
        CompressedIOUtils.NONE_COMPRESSION, CompressedIOUtils.detectCompression("124.br", false));
    assertEquals(
        CompressedIOUtils.NONE_COMPRESSION, CompressedIOUtils.detectCompression("124.csv", false));
  }

  @Test
  void isAutoCompression() {
    assertTrue(CompressedIOUtils.isAutoCompression("auto"));
  }

  @Test
  void isNoneCompression() {
    assertTrue(CompressedIOUtils.isNoneCompression("none"));
  }

  private static final List<String> CONTENT =
      new ArrayList<String>() {
        {
          add("this is");
          add("a");
          add("test file");
        }
      };

  boolean canReadContent(final String name, final String compression) throws IOException {
    Path path = Paths.get("src/test/resources/compression").resolve(name);
    return canReadContent(path, compression);
  }

  boolean canReadContent(final Path path, final String compression) throws IOException {
    URL url = path.toUri().toURL();
    LineNumberReader reader = CompressedIOUtils.newBufferedReader(url, Charsets.UTF_8, compression);
    List<String> lines = reader.lines().collect(Collectors.toList());
    assertNotNull(lines);
    assertFalse(lines.isEmpty());
    assertEquals(3, lines.size());
    assertEquals(CONTENT, lines);

    return true;
  }

  @Test
  void testReaderSupported() throws IOException {
    assertTrue(canReadContent("test-file", "none"));

    assertTrue(canReadContent("test.gz", "auto"));
    assertTrue(canReadContent("test.bz2", "auto"));
    assertTrue(canReadContent("test.lz4", "auto"));
    assertTrue(canReadContent("test.snappy", "auto"));
    assertTrue(canReadContent("test.z", "auto"));
    assertTrue(canReadContent("test.br", "auto"));
    assertTrue(canReadContent("test.lzma", "auto"));
    assertTrue(canReadContent("test.xz", "auto"));
    assertTrue(canReadContent("test.zstd", "auto"));

    assertTrue(canReadContent("test.gz", "gzip"));
  }

  @Test
  void testReaderExceptions() {
    IOException thrown =
        assertThrows(
            IOException.class,
            () -> canReadContent("test-file", "unknown"),
            "Expected error when reading with 'unknown'");
    assertTrue(thrown.getMessage().contains("Unsupported compression format: unknown"));

    thrown =
        assertThrows(
            IOException.class,
            () -> canReadContent("test.gz", "xz"),
            "Expected error when reading gzip file as XZ compressed");
    assertTrue(thrown.getMessage().contains("Can't instantiate class for compression: xz"));
  }

  @Test
  void testWriterSupported() throws IOException {
    List<String> methods =
        Arrays.asList("none", "gzip", "zstd", "xz", "lzma", "snappy", "bzip2", "lz4", "deflate");
    for (String comp : methods) {
      Path path =
          Files.createTempFile(
              "dsbulk-", "-compress" + CompressedIOUtils.getCompressionSuffix(comp));
      Files.deleteIfExists(path);
      try (BufferedWriter writer =
          CompressedIOUtils.newBufferedWriter(path.toUri().toURL(), Charsets.UTF_8, comp)) {
        for (String line : CONTENT) {
          writer.write(line);
          writer.newLine();
        }
        writer.close();
        assertTrue(canReadContent(path, comp));
      } finally {
        Files.deleteIfExists(path);
      }
    }
  }

  @Test
  void testWriterExceptions() throws IOException {
    Path path = Files.createTempFile("dsbulk-", "-compress");
    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                CompressedIOUtils.newBufferedWriter(
                    path.toUri().toURL(), Charsets.UTF_8, "unknown"),
            "Expected error when writing with 'unknown'");
    assertTrue(thrown.getMessage().contains("Unsupported compression format: unknown"));
    Files.deleteIfExists(path);
  }
}
