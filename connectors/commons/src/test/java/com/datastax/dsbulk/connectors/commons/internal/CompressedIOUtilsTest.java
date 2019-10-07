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

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CompressedIOUtilsTest {

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
        arguments("", ""));
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

  @Test
  void isNoneCompression() {
    assertTrue(CompressedIOUtils.isNoneCompression("none"));
  }

  private static final ImmutableList<String> CONTENT =
      ImmutableList.of("this is", "a", "test file");

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
        arguments("test-file", CompressedIOUtils.NONE_COMPRESSION),
        arguments("test.gz", CompressedIOUtils.GZIP_COMPRESSION),
        arguments("test.bz2", CompressedIOUtils.BZIP2_COMPRESSION),
        arguments("test.lz4", CompressedIOUtils.LZ4_COMPRESSION),
        arguments("test.snappy", CompressedIOUtils.SNAPPY_COMPRESSION),
        arguments("test.z", CompressedIOUtils.Z_COMPRESSION),
        arguments("test.br", CompressedIOUtils.BROTLI_COMPRESSION),
        arguments("test.lzma", CompressedIOUtils.LZMA_COMPRESSION),
        arguments("test.xz", CompressedIOUtils.XZ_COMPRESSION),
        arguments("test.zstd", CompressedIOUtils.ZSTD_COMPRESSION));
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
