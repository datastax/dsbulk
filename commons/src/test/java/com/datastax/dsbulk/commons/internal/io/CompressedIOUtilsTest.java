/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
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
  void should_get_correct_file_extension(String compression, String fileExtension) {
    assertThat(CompressedIOUtils.getCompressionSuffix(compression)).isEqualTo(fileExtension);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_get_correct_file_extension() {
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
  void should_support_compression_when_writing(String compression, boolean supported) {
    assertThat(CompressedIOUtils.isSupportedCompression(compression, false)).isEqualTo(supported);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_support_compression_when_writing() {
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
  void should_support_compression_when_reading(String compression, boolean supported) {
    assertThat(CompressedIOUtils.isSupportedCompression(compression, true)).isEqualTo(supported);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_support_compression_when_reading() {
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
  void should_detect_no_compression() {
    assertThat(CompressedIOUtils.isNoneCompression("none")).isTrue();
  }

  @ParameterizedTest(name = "[{index}] Should read file {0} with compression {1}")
  @MethodSource
  @DisplayName("Should read compressed file")
  void should_read_compressed_file(String filename, String compression) throws IOException {
    URL url = getClass().getResource("/compression/" + filename);
    assertCanReadCompressed(url, compression);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_read_compressed_file() {
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
  void should_throw_IOE_when_reading_compressed_file(
      String filename, String compression, String exceptionMessage) {
    URL url = getClass().getResource("/compression/" + filename);
    Throwable error = catchThrowable(() -> readCompressed(url, compression));
    assertThat(error).isInstanceOf(IOException.class).hasMessageContaining(exceptionMessage);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_throw_IOE_when_reading_compressed_file() {
    return Stream.of(
        arguments("test-file", "unknown", "Unsupported compression format: unknown"),
        arguments("test.gz", "xz", "Can't instantiate class for compression: xz"));
  }

  @ParameterizedTest(name = "[{index}] Should write file with compression {0}")
  @MethodSource
  @DisplayName("Should be able to write compressed file")
  void should_write_compressed_file(String compression) throws IOException {
    URL url = writeCompressed(compression);
    assertCanReadCompressed(url, compression);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_write_compressed_file() {
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

  @ParameterizedTest(
      name = "[{index}] Should throw exception when writing {0} with compression {1}")
  @MethodSource
  @DisplayName("Should throw exception when writing compressed file")
  void should_throw_IOE_when_writing_compressed_file(String compression, String exceptionMessage)
      throws IOException {
    Path path = Files.createTempFile("dsbulk-", "-compress");
    Files.delete(path);
    URL url = path.toUri().toURL();
    Throwable error =
        catchThrowable(() -> CompressedIOUtils.newBufferedWriter(url, Charsets.UTF_8, compression));
    assertThat(error).isInstanceOf(IOException.class).hasMessageContaining(exceptionMessage);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_throw_IOE_when_writing_compressed_file() {
    return Stream.of(
        arguments("unknown", "Unsupported compression format: unknown"),
        arguments("z", "Unsupported compression format: z"));
  }

  private static void assertCanReadCompressed(URL url, String compression) throws IOException {
    List<String> lines = readCompressed(url, compression);
    assertThat(lines)
        .isNotNull()
        .isNotEmpty()
        .hasSize(3)
        .containsExactly("this is", "a", "test file");
  }

  private static List<String> readCompressed(URL url, String compression) throws IOException {
    LineNumberReader reader = CompressedIOUtils.newBufferedReader(url, Charsets.UTF_8, compression);
    return reader.lines().collect(Collectors.toList());
  }

  private static URL writeCompressed(String compression) throws IOException {
    Path path =
        Files.createTempFile(
            "dsbulk-", "-compress" + CompressedIOUtils.getCompressionSuffix(compression));
    Files.delete(path);
    URL url = path.toUri().toURL();
    try (BufferedWriter writer =
        CompressedIOUtils.newBufferedWriter(url, Charsets.UTF_8, compression)) {
      writer.write("this is");
      writer.newLine();
      writer.write("a");
      writer.newLine();
      writer.write("test file");
      writer.newLine();
    }
    return url;
  }
}
