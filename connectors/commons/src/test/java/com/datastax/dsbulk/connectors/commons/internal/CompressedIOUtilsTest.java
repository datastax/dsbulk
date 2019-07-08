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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CompresssedCompressedIOUtilsTest {

  @ParameterizedTest(name = "[{index}] Compression {0} has correct file extension: {1}")
  @MethodSource
  @DisplayName("Should get correct file extension")
  void getCompressionSuffix(final String compType, final String fileExtension) {
    assertEquals(fileExtension, CompressedIOUtils.getCompressionSuffix(compType));
  }

  private static Stream<Arguments> getCompressionSuffix() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION, ".gz"),
        arguments(CompressedIOUtils.XZ_COMPRESSION, ".xz"),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION, ".bz2"),
        arguments(CompressedIOUtils.LZMA_COMPRESSION, ".lzma"),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION, ".zstd"),
        arguments(CompressedIOUtils.LZ4_COMPRESSION, ".lz4"),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION, ".snappy"),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION, ".deflate"),
        arguments(CompressedIOUtils.AUTO_COMPRESSION, ""),
        arguments("", ""));
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

  @ParameterizedTest(name = "[{index}] Is Compression {0} supported for write? {1}")
  @MethodSource
  @DisplayName("Compression should be supported for write")
  void isSupportedWriteCompression(final String compType, boolean supported) {
    assertEquals(supported, CompressedIOUtils.isSupportedCompression(compType, false));
  }

  private static Stream<Arguments> isSupportedWriteCompression() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION, true),
        arguments(CompressedIOUtils.XZ_COMPRESSION, true),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION, true),
        arguments(CompressedIOUtils.LZMA_COMPRESSION, true),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION, true),
        arguments(CompressedIOUtils.LZ4_COMPRESSION, true),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION, true),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION, true),
        arguments(CompressedIOUtils.BROTLI_COMPRESSION, false),
        arguments(CompressedIOUtils.Z_COMPRESSION, false),
        arguments(CompressedIOUtils.DEFLATE64_COMPRESSION, false),
        arguments(null, false),
        arguments("abc", false));
  }

  @ParameterizedTest(name = "[{index}] Is Compression {0} supported for read? {1}")
  @MethodSource
  @DisplayName("Compression should be supported for read")
  void isSupportedReadCompression(final String compType, boolean supported) {
    assertEquals(supported, CompressedIOUtils.isSupportedCompression(compType, true));
  }

  private static Stream<Arguments> isSupportedReadCompression() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION, true),
        arguments(CompressedIOUtils.XZ_COMPRESSION, true),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION, true),
        arguments(CompressedIOUtils.LZMA_COMPRESSION, true),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION, true),
        arguments(CompressedIOUtils.LZ4_COMPRESSION, true),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION, true),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION, true),
        arguments(CompressedIOUtils.BROTLI_COMPRESSION, true),
        arguments(CompressedIOUtils.Z_COMPRESSION, true),
        arguments(CompressedIOUtils.DEFLATE64_COMPRESSION, true),
        arguments(null, false),
        arguments("abc", false));
  }

  @ParameterizedTest(name = "[{index}] Compression for reading {1} should be {0}")
  @MethodSource
  @DisplayName("Compression should be detected for reads")
  void detectReadCompression(final String compType, final String filename) {
    assertEquals(compType, CompressedIOUtils.detectCompression(filename, true));
  }

  private static Stream<Arguments> detectReadCompression() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION, "124.gz"),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION, "124.zstd"),
        arguments(CompressedIOUtils.BROTLI_COMPRESSION, "124.br"),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION, "124.bz2"),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION, "124.deflate"),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION, "124.snappy"),
        arguments(CompressedIOUtils.XZ_COMPRESSION, "124.xz"),
        arguments(CompressedIOUtils.LZ4_COMPRESSION, "124.lz4"),
        arguments(CompressedIOUtils.LZMA_COMPRESSION, "124.lzma"),
        arguments(CompressedIOUtils.Z_COMPRESSION, "124.z"),
        arguments(CompressedIOUtils.NONE_COMPRESSION, "124.csv"));
  }

  @ParameterizedTest(name = "[{index}] Compression for reading {1} should be {0}")
  @MethodSource
  @DisplayName("Compression should be detected for writes")
  void detectWriteCompression(final String compType, final String filename) {
    assertEquals(compType, CompressedIOUtils.detectCompression(filename, false));
  }

  private static Stream<Arguments> detectWriteCompression() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION, "124.gz"),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION, "124.zstd"),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION, "124.bz2"),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION, "124.deflate"),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION, "124.snappy"),
        arguments(CompressedIOUtils.XZ_COMPRESSION, "124.xz"),
        arguments(CompressedIOUtils.LZ4_COMPRESSION, "124.lz4"),
        arguments(CompressedIOUtils.LZMA_COMPRESSION, "124.lzma"),
        arguments(CompressedIOUtils.NONE_COMPRESSION, "124.br"),
        arguments(CompressedIOUtils.NONE_COMPRESSION, "124.csv"));
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

  @ParameterizedTest(name = "[{index}] Should read file {0} with compression {1}")
  @MethodSource
  @DisplayName("Should read compressed file")
  void testReaderSupported(final String filename, final String compType) throws IOException {
    assertTrue(canReadContent(filename, compType));
  }

  private static Stream<Arguments> testReaderSupported() {
    return Stream.of(
        arguments("test-file", "none"),
        arguments("test.gz", "auto"),
        arguments("test.bz2", "auto"),
        arguments("test.lz4", "auto"),
        arguments("test.snappy", "auto"),
        arguments("test.z", "auto"),
        arguments("test.br", "auto"),
        arguments("test.lzma", "auto"),
        arguments("test.xz", "auto"),
        arguments("test.zstd", "auto"),
        arguments("test.gz", "gzip"));
  }

  @ParameterizedTest(
      name = "[{index}] Should throw exception when reading {0} with compression {1}")
  @MethodSource
  @DisplayName("Should throw exception when reading compressed file")
  void testReaderExceptions(
      final String filename,
      final String compMethod,
      final String description,
      final String exceptionMessage) {
    IOException thrown =
        assertThrows(IOException.class, () -> canReadContent(filename, compMethod), description);
    assertTrue(thrown.getMessage().contains(exceptionMessage));
  }

  private static Stream<Arguments> testReaderExceptions() {
    return Stream.of(
        arguments(
            "test-file",
            "unknown",
            "Expected error when reading with 'unknown'",
            "Unsupported compression format: unknown"),
        arguments(
            "test.gz",
            "xz",
            "Expected error when reading gzip file as XZ compressed",
            "Can't instantiate class for compression: xz"));
  }

  @ParameterizedTest(name = "[{index}] Should write file with compression {0}")
  @MethodSource
  @DisplayName("Should be able to write compressed file")
  void testWriterSupported(final String compMethod) throws IOException {
    Path path =
        Files.createTempFile(
            "dsbulk-", "-compress" + CompressedIOUtils.getCompressionSuffix(compMethod));
    Files.deleteIfExists(path);
    try (BufferedWriter writer =
        CompressedIOUtils.newBufferedWriter(path.toUri().toURL(), Charsets.UTF_8, compMethod)) {
      for (String line : CONTENT) {
        writer.write(line);
        writer.newLine();
      }
      writer.close();
      assertTrue(canReadContent(path, compMethod));
    } finally {
      Files.deleteIfExists(path);
    }
  }

  private static Stream<Arguments> testWriterSupported() {
    return Stream.of(
        arguments("none"),
        arguments("gzip"),
        arguments("zstd"),
        arguments("xz"),
        arguments("lzma"),
        arguments("snappy"),
        arguments("bzip2"),
        arguments("lz4"),
        arguments("deflate"));
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
