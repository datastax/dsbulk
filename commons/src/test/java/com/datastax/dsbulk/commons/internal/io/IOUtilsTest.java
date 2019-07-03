/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.io;

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

class IOUtilsTest {

  @Test
  void getCompressionSuffix() {
    assertEquals(".gz", IOUtils.getCompressionSuffix(IOUtils.GZIP_COMPRESSION));
    assertEquals(".xz", IOUtils.getCompressionSuffix(IOUtils.XZ_COMPRESSION));
    assertEquals(".bz2", IOUtils.getCompressionSuffix(IOUtils.BZIP2_COMPRESSION));
    assertEquals(".lzma", IOUtils.getCompressionSuffix(IOUtils.LZMA_COMPRESSION));
    assertEquals(".zstd", IOUtils.getCompressionSuffix(IOUtils.ZSTD_COMPRESSION));
    assertEquals(".lz4", IOUtils.getCompressionSuffix(IOUtils.LZ4_COMPRESSION));
    assertEquals(".snappy", IOUtils.getCompressionSuffix(IOUtils.SNAPPY_COMPRESSION));
    assertEquals(".deflate", IOUtils.getCompressionSuffix(IOUtils.DEFLATE_COMPRESSION));

    assertEquals("", IOUtils.getCompressionSuffix(IOUtils.AUTO_COMPRESSION));
    assertEquals("", IOUtils.getCompressionSuffix("abc"));
  }

  @Test
  void getSupportedCompressions() {
    Set<String> s = new HashSet<>(IOUtils.getSupportedCompressions(true));
    assertTrue(s.contains(IOUtils.AUTO_COMPRESSION));
    assertTrue(s.contains(IOUtils.NONE_COMPRESSION));
    assertTrue(s.contains(IOUtils.XZ_COMPRESSION));
    assertTrue(s.contains(IOUtils.GZIP_COMPRESSION));
    assertTrue(s.contains(IOUtils.ZSTD_COMPRESSION));
    assertTrue(s.contains(IOUtils.BZIP2_COMPRESSION));
    assertTrue(s.contains(IOUtils.SNAPPY_COMPRESSION));
    assertTrue(s.contains(IOUtils.LZ4_COMPRESSION));
    assertTrue(s.contains(IOUtils.LZMA_COMPRESSION));
    assertTrue(s.contains(IOUtils.BROTLI_COMPRESSION));
    assertTrue(s.contains(IOUtils.DEFLATE_COMPRESSION));
    assertTrue(s.contains(IOUtils.DEFLATE64_COMPRESSION));
    assertTrue(s.contains(IOUtils.Z_COMPRESSION));

    //
    s.clear();
    s.addAll(IOUtils.getSupportedCompressions(false));
    assertTrue(s.contains(IOUtils.AUTO_COMPRESSION));
    assertTrue(s.contains(IOUtils.NONE_COMPRESSION));
    assertTrue(s.contains(IOUtils.XZ_COMPRESSION));
    assertTrue(s.contains(IOUtils.GZIP_COMPRESSION));
    assertTrue(s.contains(IOUtils.ZSTD_COMPRESSION));
    assertTrue(s.contains(IOUtils.BZIP2_COMPRESSION));
    assertTrue(s.contains(IOUtils.SNAPPY_COMPRESSION));
    assertTrue(s.contains(IOUtils.LZ4_COMPRESSION));
    assertTrue(s.contains(IOUtils.LZMA_COMPRESSION));
    assertFalse(s.contains(IOUtils.BROTLI_COMPRESSION));
    assertTrue(s.contains(IOUtils.DEFLATE_COMPRESSION));
    assertFalse(s.contains(IOUtils.DEFLATE64_COMPRESSION));
    assertFalse(s.contains(IOUtils.Z_COMPRESSION));
  }

  @Test
  void isSupportedCompression() {
    // supported for both reads & writes
    assertTrue(IOUtils.isSupportedCompression(IOUtils.XZ_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.XZ_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.GZIP_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.GZIP_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.ZSTD_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.ZSTD_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.BZIP2_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.BZIP2_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.SNAPPY_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.SNAPPY_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.LZ4_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.LZ4_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.LZMA_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.LZMA_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.DEFLATE_COMPRESSION, true));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.DEFLATE_COMPRESSION, false));

    // supported only for reads
    assertTrue(IOUtils.isSupportedCompression(IOUtils.BROTLI_COMPRESSION, true));
    assertFalse(IOUtils.isSupportedCompression(IOUtils.BROTLI_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.Z_COMPRESSION, true));
    assertFalse(IOUtils.isSupportedCompression(IOUtils.Z_COMPRESSION, false));
    assertTrue(IOUtils.isSupportedCompression(IOUtils.DEFLATE64_COMPRESSION, true));
    assertFalse(IOUtils.isSupportedCompression(IOUtils.DEFLATE64_COMPRESSION, false));

    // unknown compressions
    assertFalse(IOUtils.isSupportedCompression(null, true));
    assertFalse(IOUtils.isSupportedCompression(null, false));
    assertFalse(IOUtils.isSupportedCompression("abc", true));
    assertFalse(IOUtils.isSupportedCompression("abc", false));
  }

  @Test
  void detectCompression() {
    // for reading
    assertEquals(IOUtils.GZIP_COMPRESSION, IOUtils.detectCompression("124.gz", true));
    assertEquals(IOUtils.ZSTD_COMPRESSION, IOUtils.detectCompression("124.zstd", true));
    assertEquals(IOUtils.BROTLI_COMPRESSION, IOUtils.detectCompression("124.br", true));
    assertEquals(IOUtils.BZIP2_COMPRESSION, IOUtils.detectCompression("124.bz2", true));
    assertEquals(IOUtils.DEFLATE_COMPRESSION, IOUtils.detectCompression("124.deflate", true));
    assertEquals(IOUtils.SNAPPY_COMPRESSION, IOUtils.detectCompression("124.snappy", true));
    assertEquals(IOUtils.XZ_COMPRESSION, IOUtils.detectCompression("124.xz", true));
    assertEquals(IOUtils.LZ4_COMPRESSION, IOUtils.detectCompression("124.lz4", true));
    assertEquals(IOUtils.LZMA_COMPRESSION, IOUtils.detectCompression("124.lzma", true));
    assertEquals(IOUtils.Z_COMPRESSION, IOUtils.detectCompression("124.z", true));
    assertEquals(IOUtils.NONE_COMPRESSION, IOUtils.detectCompression("124.csv", true));

    // for writing
    assertEquals(IOUtils.GZIP_COMPRESSION, IOUtils.detectCompression("124.gz", false));
    assertEquals(IOUtils.ZSTD_COMPRESSION, IOUtils.detectCompression("124.zstd", false));
    assertEquals(IOUtils.BZIP2_COMPRESSION, IOUtils.detectCompression("124.bz2", false));
    assertEquals(IOUtils.DEFLATE_COMPRESSION, IOUtils.detectCompression("124.deflate", false));
    assertEquals(IOUtils.SNAPPY_COMPRESSION, IOUtils.detectCompression("124.snappy", false));
    assertEquals(IOUtils.XZ_COMPRESSION, IOUtils.detectCompression("124.xz", false));
    assertEquals(IOUtils.LZ4_COMPRESSION, IOUtils.detectCompression("124.lz4", false));
    assertEquals(IOUtils.LZMA_COMPRESSION, IOUtils.detectCompression("124.lzma", false));
    assertEquals(IOUtils.NONE_COMPRESSION, IOUtils.detectCompression("124.br", false));
    assertEquals(IOUtils.NONE_COMPRESSION, IOUtils.detectCompression("124.csv", false));
  }

  @Test
  void isAutoCompression() {
    assertTrue(IOUtils.isAutoCompression("auto"));
  }

  @Test
  void isNoneCompression() {
    assertTrue(IOUtils.isNoneCompression("none"));
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
    LineNumberReader reader = IOUtils.newBufferedReader(url, Charsets.UTF_8, compression);
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
  void testReaderExceptions() throws IOException {
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
      Path path = Files.createTempFile("dsbulk-", "-compress" + IOUtils.getCompressionSuffix(comp));
      Files.deleteIfExists(path);
      try (BufferedWriter writer =
          IOUtils.newBufferedWriter(path.toUri().toURL(), Charsets.UTF_8, comp)) {
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
            () -> IOUtils.newBufferedWriter(path.toUri().toURL(), Charsets.UTF_8, "unknown"),
            "Expected error when writing with 'unknown'");
    assertTrue(thrown.getMessage().contains("Unsupported compression format: unknown"));
    Files.deleteIfExists(path);
  }
}
