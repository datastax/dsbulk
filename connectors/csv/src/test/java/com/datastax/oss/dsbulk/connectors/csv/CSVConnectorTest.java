/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.connectors.csv;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Throwables.getRootCause;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.dsbulk.compression.CompressedIOUtils;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.datastax.oss.dsbulk.tests.utils.URLUtils;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.typesafe.config.Config;
import com.univocity.parsers.common.TextParsingException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import ru.lanwen.wiremock.ext.WiremockResolver;
import ru.lanwen.wiremock.ext.WiremockResolver.Wiremock;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(WiremockResolver.class)
class CSVConnectorTest {

  private static final int IRRELEVANT_POSITION = -1;

  static {
    URLUtils.setURLFactoryIfNeeded();
    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {});
  }

  private static Path MULTIPLE_URLS_FILE;

  private final URI resource = URI.create("file://file1.csv");

  @BeforeAll
  static void setup() throws IOException {
    MULTIPLE_URLS_FILE =
        FileUtils.createURLFile(
            Arrays.asList(
                rawURL("/part_1"), rawURL("/part_2"), rawURL("/root-custom/child/part-0003")));
  }

  @AfterAll
  static void cleanup() throws IOException {
    Files.delete(MULTIPLE_URLS_FILE);
  }

  @ParameterizedTest(name = "[{index}] read {0} with compression {1}")
  @MethodSource
  @DisplayName("Should read single file with given compression")
  void should_read_single_file(String fileName, String compMethod) throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/" + fileName),
            "normalizeLineEndingsInQuotes",
            true,
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"",
            "compression",
            StringUtils.quoteJson(compMethod));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertRecords(actual);
    connector.close();
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_read_single_file() {
    return Stream.of(
        arguments("sample.csv", CompressedIOUtils.NONE_COMPRESSION),
        arguments("sample.csv.gz", CompressedIOUtils.GZIP_COMPRESSION),
        arguments("sample.csv.bz2", CompressedIOUtils.BZIP2_COMPRESSION),
        arguments("sample.csv.lz4", CompressedIOUtils.LZ4_COMPRESSION),
        arguments("sample.csv.snappy", CompressedIOUtils.SNAPPY_COMPRESSION),
        arguments("sample.csv.z", CompressedIOUtils.Z_COMPRESSION),
        arguments("sample.csv.br", CompressedIOUtils.BROTLI_COMPRESSION),
        arguments("sample.csv.lzma", CompressedIOUtils.LZMA_COMPRESSION),
        arguments("sample.csv.xz", CompressedIOUtils.XZ_COMPRESSION),
        arguments("sample.csv.zstd", CompressedIOUtils.ZSTD_COMPRESSION));
  }

  @Test
  void should_read_single_file_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/sample.csv"),
            "normalizeLineEndingsInQuotes",
            true,
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"");
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.merge(connector.readByResource()).collectList().block();
    assertRecords(actual);
    connector.close();
  }

  private static void assertRecords(List<Record> actual) {
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsExactly(
            "1997",
            "Ford",
            "E350",
            "  ac, abs, moon  ",
            "3000.00", // mapped
            "1997",
            "Ford",
            "E350",
            "  ac, abs, moon  ",
            "3000.00"); // indexed
    assertThat(actual.get(1).values())
        .containsExactly(
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            "",
            "4900.00",
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            "",
            "4900.00");
    assertThat(actual.get(2).values())
        .containsExactly(
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00",
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00");
    assertThat(actual.get(3).values())
        .containsExactly(
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            null,
            "5000.00",
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            null,
            "5000.00");
    assertThat(actual.get(4).values())
        .containsExactly(
            null,
            null,
            "Venture \"Extended Edition\"",
            "",
            "4900.00",
            null,
            null,
            "Venture \"Extended Edition\"",
            "",
            "4900.00");
    assertThat(actual.get(0).getPosition()).isEqualTo(1L);
    assertThat(actual.get(1).getPosition()).isEqualTo(2L);
    assertThat(actual.get(2).getPosition()).isEqualTo(3L);
    assertThat(actual.get(3).getPosition()).isEqualTo(4L);
    assertThat(actual.get(4).getPosition()).isEqualTo(5L);
  }

  @Test
  void should_read_from_stdin_with_special_encoding() throws Exception {
    InputStream stdin = System.in;
    try {
      String line = "fóô,bàr,qïx\n";
      InputStream is = new ByteArrayInputStream(line.getBytes(ISO_8859_1));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "header", false, "url", "-", "encoding", "ISO-8859-1");
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(line);
      assertThat(actual.get(0).getResource()).isEqualTo(URI.create("std:/"));
      assertThat(actual.get(0).getPosition()).isEqualTo(1L);
      assertThat(actual.get(0).values()).containsExactly("fóô", "bàr", "qïx");
      connector.close();
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  void should_write_to_stdout_with_special_encoding() throws Exception {
    PrintStream stdout = System.out;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      System.setOut(out);
      CSVConnector connector = new CSVConnector();
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "header", false, "encoding", "ISO-8859-1");
      connector.configure(settings, false);
      connector.init();
      Flux.<Record>just(
              DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, "fóô", "bàr", "qïx"))
          .transform(connector.write())
          .blockLast();
      connector.close();
      assertThat(new String(baos.toByteArray(), ISO_8859_1))
          .isEqualTo("fóô,bàr,qïx" + System.lineSeparator());
    } finally {
      System.setOut(stdout);
    }
  }

  @Test
  void should_read_from_stdin_with_special_newline() throws Exception {
    InputStream stdin = System.in;
    try {
      String line = "abc,de\nf,ghk\r\n";
      InputStream is = new ByteArrayInputStream(line.getBytes(UTF_8));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "header", false, "url", "-", "newline", "\"\\r\\n\"");
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(line);
      assertThat(actual.get(0).values()).containsExactly("abc", "de\nf", "ghk");
      connector.close();
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  void should_write_to_stdout_with_special_newline() throws Exception {
    PrintStream stdout = System.out;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      System.setOut(out);
      CSVConnector connector = new CSVConnector();
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "header", false, "newline", "\"\\r\\n\"");
      connector.configure(settings, false);
      connector.init();
      Flux.<Record>just(
              DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, "abc", "de\nf", "ghk"))
          .transform(connector.write())
          .blockLast();
      connector.close();
      assertThat(new String(baos.toByteArray(), UTF_8)).isEqualTo("abc,\"de\nf\",ghk\r\n");
    } finally {
      System.setOut(stdout);
    }
  }

  @Test
  void should_read_all_resources_in_directory() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_with_path() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Paths.get(getClass().getResource("/root").toURI());
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", StringUtils.quoteJson(rootPath), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_scan_directory_recursively_with_custom_file_name_format() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/root-custom"),
            "recursive",
            true,
            "fileNamePattern",
            "\"**/part-*\"");
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_warn_when_directory_empty(@LogCapture LogInterceptor logs) throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Files.createTempDirectory("empty");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(rootPath),
              "recursive",
              true,
              "fileNamePattern",
              "\"**/part-*\"");
      connector.configure(settings, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(String.format("Directory %s has no readable files.", rootPath));
      connector.close();
    } finally {
      FileUtils.deleteDirectory(rootPath);
    }
  }

  @Test
  void should_warn_when_no_files_matched(@LogCapture LogInterceptor logs) throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(rootPath),
              "recursive",
              true,
              "fileNamePattern",
              "\"**/part-*\"");
      connector.configure(settings, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(
              String.format(
                  "No files in directory %s matched the connector.csv.fileNamePattern of \"**/part-*\".",
                  rootPath));
      connector.close();
    } finally {
      FileUtils.deleteDirectory(rootPath);
    }
  }

  @ParameterizedTest(name = "[{index}] Should get correct message when compression {0} is used")
  @MethodSource
  @DisplayName(
      "Should get correct exception message when there are no matching files with compression enabled")
  void should_warn_when_no_files_matched_and_compression_enabled(
      String compression, @LogCapture LogInterceptor logs) throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(rootPath),
              "recursive",
              true,
              "compression",
              StringUtils.quoteJson(compression));
      connector.configure(settings, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(
              String.format(
                  "No files in directory %s matched the connector.csv.fileNamePattern of \"**/*.csv%s\".",
                  rootPath, CompressedIOUtils.getCompressionSuffix(compression)));
      connector.close();
    } finally {
      FileUtils.deleteDirectory(rootPath);
    }
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_warn_when_no_files_matched_and_compression_enabled() {
    return Stream.of(
        arguments(CompressedIOUtils.GZIP_COMPRESSION),
        arguments(CompressedIOUtils.XZ_COMPRESSION),
        arguments(CompressedIOUtils.BZIP2_COMPRESSION),
        arguments(CompressedIOUtils.LZMA_COMPRESSION),
        arguments(CompressedIOUtils.ZSTD_COMPRESSION),
        arguments(CompressedIOUtils.LZ4_COMPRESSION),
        arguments(CompressedIOUtils.SNAPPY_COMPRESSION),
        arguments(CompressedIOUtils.DEFLATE_COMPRESSION));
  }

  @Test
  void should_write_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1);
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_compressed_gzip() throws Exception {
    CSVConnector connector = new CSVConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "compression",
              "\"gzip\"");
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      Path outPath = out.resolve("output-000001.csv.gz");
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  new GZIPInputStream(Files.newInputStream(outPath)), Charsets.UTF_8));
      List<String> actual = reader.lines().collect(Collectors.toList());
      reader.close();
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_compressed_gzip_custom_file_format() throws Exception {
    CSVConnector connector = new CSVConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "compression",
              "\"gzip\"",
              "fileNameFormat",
              "file%d");
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      Path outPath = out.resolve("file1");
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  new GZIPInputStream(Files.newInputStream(outPath)), Charsets.UTF_8));
      List<String> actual = reader.lines().collect(Collectors.toList());
      reader.close();
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  void should_write_multiple_files() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              4);
      connector.configure(settings, false);
      connector.init();
      // repeat the records 200 times to fully exercise multiple file writing
      Flux.fromIterable(createRecords()).repeat(200).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual =
          FileUtils.readAllLinesInDirectoryAsStream(out)
              .sorted()
              .distinct()
              .collect(Collectors.toList());
      assertThat(actual)
          .containsExactly(
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              "Year,Make,Model,Description,Price",
              "air, moon roof, loaded\",4799.00");
    } finally {
      FileUtils.deleteDirectory(out);
    }
  }

  // Test for DAT-443
  @Test
  void should_generate_file_name() throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "url", StringUtils.quoteJson(out));
    connector.configure(settings, false);
    connector.init();
    AtomicInteger counter = (AtomicInteger) ReflectionUtils.getInternalState(connector, "counter");
    Method getOrCreateDestinationURL =
        ReflectionUtils.locateMethod("getOrCreateDestinationURL", CSVConnector.class, 0);
    counter.set(999);
    URL nextFile = ReflectionUtils.invokeMethod(getOrCreateDestinationURL, connector, URL.class);
    assertThat(nextFile.getPath()).endsWith("output-001000.csv");
    counter.set(999_999);
    nextFile = ReflectionUtils.invokeMethod(getOrCreateDestinationURL, connector, URL.class);
    assertThat(nextFile.getPath()).endsWith("output-1000000.csv");
  }

  @Test
  void should_roll_file_when_max_lines_reached() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv",
              "url",
              StringUtils.quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "maxRecords",
              4);
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      List<String> csv1 = Files.readAllLines(out.resolve("output-000001.csv"));
      List<String> csv2 = Files.readAllLines(out.resolve("output-000002.csv"));
      assertThat(csv1)
          .hasSize(5); // 1 header + 4 lines (3 records actually, but one record spans over 2 lines)
      assertThat(csv2).hasSize(3); // 1 header + 2 lines
      assertThat(csv1)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00");
      assertThat(csv2)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      FileUtils.deleteDirectory(out);
    }
  }

  @Test
  void should_return_unmappable_record_when_line_malformed() throws Exception {
    InputStream stdin = System.in;
    try {
      String lines = "header1,header2\nvalue1,value2,value3";
      InputStream is = new ByteArrayInputStream(lines.getBytes(UTF_8));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "header", true);
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0)).isInstanceOf(ErrorRecord.class);
      assertThat(actual.get(0).getSource()).isEqualTo("value1,value2,value3");
      assertThat(((ErrorRecord) actual.get(0)).getError())
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(actual.get(0).values()).isEmpty();
      connector.close();
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  void should_skip_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true, "skipRecords", 10);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(450);
    connector.close();
  }

  @Test
  void should_skip_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true, "skipRecords", 150);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(0);
    connector.close();
  }

  @Test
  void should_honor_max_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true, "maxRecords", 10);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(50);
    connector.close();
  }

  @Test
  void should_honor_max_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/root"), "recursive", true, "maxRecords", 1);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(5);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/root"),
            "recursive",
            true,
            "skipRecords",
            95,
            "maxRecords",
            10);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(25);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/root/ip-by-country-sample1.csv"),
            "skipRecords",
            10,
            "maxRecords",
            1);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getSource().toString().trim())
        .isEqualTo(
            "\"212.63.180.20\",\"212.63.180.23\",\"3560944660\",\"3560944663\",\"MZ\",\"Mozambique\"");
    connector.close();
  }

  @Test
  void should_honor_ignoreLeadingWhitespaces_and_ignoreTrailingWhitespaces_when_reading()
      throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton(" foo "));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "ignoreLeadingWhitespaces",
            false,
            "ignoreTrailingWhitespaces",
            false,
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo(" foo ");
    connector.close();
  }

  @Test
  void should_honor_ignoreLeadingWhitespaces_and_ignoreTrailingWhitespaces_when_reading2()
      throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton(" foo "));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "ignoreLeadingWhitespaces",
            true,
            "ignoreTrailingWhitespaces",
            true,
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo("foo");
    connector.close();
  }

  @Test
  void should_honor_ignoreLeadingWhitespaces_and_ignoreTrailingWhitespaces_when_writing()
      throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(out),
            "ignoreLeadingWhitespaces",
            false,
            "ignoreTrailingWhitespaces",
            false,
            "maxConcurrentFiles",
            1,
            "header",
            false);
    connector.configure(settings, false);
    connector.init();
    Flux.<Record>just(DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, " foo "))
        .transform(connector.write())
        .blockFirst();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual).hasSize(1).containsExactly(" foo ");
  }

  @Test
  void should_honor_ignoreLeadingWhitespaces_and_ignoreTrailingWhitespaces_when_writing2()
      throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(out),
            "ignoreLeadingWhitespaces",
            true,
            "ignoreTrailingWhitespaces",
            true,
            "header",
            false);
    connector.configure(settings, false);
    connector.init();
    Flux.<Record>just(DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, " foo "))
        .transform(connector.write())
        .blockFirst();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual).hasSize(1).containsExactly("foo");
  }

  @Test
  void should_honor_ignoreLeadingWhitespacesInQuotes_and_ignoreTrailingWhitespacesInQuotes()
      throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton("\" foo \""));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "ignoreLeadingWhitespacesInQuotes",
            false,
            "ignoreTrailingWhitespacesInQuotes",
            false,
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo(" foo ");
    connector.close();
  }

  @Test
  void should_honor_ignoreLeadingWhitespacesInQuotes_and_ignoreTrailingWhitespacesInQuotes2()
      throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton("\" foo \""));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "ignoreLeadingWhitespacesInQuotes",
            true,
            "ignoreTrailingWhitespacesInQuotes",
            true,
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo("foo");
    connector.close();
  }

  @Test
  void should_honor_nullValue_when_reading() throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton(","));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "nullValue",
            null,
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isNull();
    connector.close();
  }

  @Test
  void should_honor_nullValue_when_reading2() throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton(","));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "nullValue",
            "NULL",
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo("NULL");
    connector.close();
  }

  @Test
  void should_honor_nullValue_when_writing() throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(out),
            "nullValue",
            null,
            "header",
            true);
    connector.configure(settings, false);
    connector.init();
    Flux.<Record>just(
            DefaultRecord.mapped(
                "source",
                resource,
                IRRELEVANT_POSITION,
                new Field[] {new DefaultMappedField("field1")},
                new Object[] {null}))
        .transform(connector.write())
        .blockFirst();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual)
        .hasSize(1)
        .containsExactly("field1"); // only the header line should have been printed
  }

  @Test
  void should_honor_nullValue_when_writing2() throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(out),
            "nullValue",
            "NULL",
            "header",
            false);
    connector.configure(settings, false);
    connector.init();
    Flux.<Record>just(
            DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, new Object[] {null}))
        .transform(connector.write())
        .blockFirst();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual).hasSize(1).containsExactly("NULL");
  }

  @Test
  void should_honor_emptyValue_when_reading() throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton("\"\""));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "emptyValue",
            "\"\"",
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo("");
    connector.close();
  }

  @Test
  void should_honor_emptyValue_when_reading2() throws Exception {
    Path file = Files.createTempFile("test", ".csv");
    Files.write(file, Collections.singleton("\"\""));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(file),
            "emptyValue",
            "EMPTY",
            "header",
            false);
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getFieldValue(new DefaultIndexedField(0))).isEqualTo("EMPTY");
    connector.close();
  }

  @Test()
  void should_error_when_directory_is_not_empty() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Path file = out.resolve("output-000001.csv");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "url", StringUtils.quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false);
      assertThrows(IllegalArgumentException.class, connector::init);
    } finally {
      FileUtils.deleteDirectory(out);
    }
  }

  @Test()
  void should_error_when_newline_is_wrong() {
    CSVConnector connector = new CSVConnector();
    // empty string test
    Config settings1 = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "newline", "\"\"");
    assertThrows(IllegalArgumentException.class, () -> connector.configure(settings1, false));
    // long string test
    Config settings2 =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "newline", "\"abc\"");
    assertThrows(IllegalArgumentException.class, () -> connector.configure(settings2, false));
  }

  @Test
  void should_abort_write_single_file_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "url", StringUtils.quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false);
      connector.init();
      Path file = out.resolve("output-000001.csv");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      assertThatThrownBy(
              () -> Flux.fromIterable(createRecords()).transform(connector.write()).blockLast())
          .hasRootCauseExactlyInstanceOf(FileAlreadyExistsException.class);
      connector.close();
    } finally {
      FileUtils.deleteDirectory(out);
    }
  }

  @Test
  void should_abort_write_multiple_files_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.csv", "url", StringUtils.quoteJson(out), "maxConcurrentFiles", 2);
      connector.configure(settings, false);
      connector.init();
      Path file1 = out.resolve("output-000001.csv");
      Path file2 = out.resolve("output-000002.csv");
      // will cause the write workers to fail because the files already exist
      Files.createFile(file1);
      Files.createFile(file2);
      /*
      Two situations can happen:
      1) the two exceptions are caught and returned by Reactor the following way:
        reactor.core.Exceptions$CompositeException: Multiple exceptions
          Suppressed: java.io.UncheckedIOException: Error opening file:/tmp/test2860250719180574800/output-000001.json
          Caused by: java.nio.file.FileAlreadyExistsException: /tmp/test2860250719180574800/output-000001.json
          Suppressed: java.io.UncheckedIOException: Error opening file:/tmp/test2860250719180574800/output-000002.json
          Caused by: java.nio.file.FileAlreadyExistsException: /tmp/test2860250719180574800/output-000002.json
          Suppressed: java.lang.Exception: #block terminated with an error
      2) Only one exception is caught:
        java.io.UncheckedIOException: Error opening file:/tmp/test2860250719180574800/output-000001.json
          Caused by: java.nio.file.FileAlreadyExistsException: /tmp/test2860250719180574800/output-000001.json
      */
      assertThatThrownBy(
              () ->
                  Flux.fromIterable(createRecords())
                      .repeat(100)
                      .transform(connector.write())
                      .blockLast())
          .satisfies(
              t ->
                  assertThat(
                          getRootCause(t) instanceof FileAlreadyExistsException
                              || Arrays.stream(t.getSuppressed())
                                  .map(Throwables::getRootCause)
                                  .anyMatch(FileAlreadyExistsException.class::isInstance))
                      .isTrue());
      connector.close();
    } finally {
      FileUtils.deleteDirectory(out);
    }
  }

  @Test
  void should_read_from_http_url(@Wiremock WireMockServer server) throws Exception {
    server.givenThat(
        any(urlPathEqualTo("/file.csv"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/csv")
                    .withBody(FileUtils.readFile(path("/sample.csv")))));
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            String.format("\"%s/file.csv\"", server.baseUrl()),
            "normalizeLineEndingsInQuotes",
            true,
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"");
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertRecords(actual);
    connector.close();
  }

  @Test
  void should_not_write_to_http_url() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", "\"http://localhost:1234/file.csv\"");
    connector.configure(settings, false);
    connector.init();
    assertThatThrownBy(
            () -> Flux.fromIterable(createRecords()).transform(connector.write()).blockLast())
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining(
                        "HTTP/HTTPS protocols cannot be used for output: http://localhost:1234/file.csv"));
    connector.close();
  }

  @Test
  void should_throw_IOE_when_max_chars_per_column_exceeded() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/sample.csv"),
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"",
            "maxCharsPerColumn",
            15);
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .satisfies(
            t ->
                assertThat(t.getCause())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                        "Length of parsed input (16) exceeds the maximum number "
                            + "of characters defined in your parser settings (15). "
                            + "Please increase the value of the connector.csv.maxCharsPerColumn setting.")
                    .hasCauseExactlyInstanceOf(TextParsingException.class)
                    .hasRootCauseExactlyInstanceOf(ArrayIndexOutOfBoundsException.class));
    connector.close();
  }

  @Test
  void should_throw_IOE_when_max_columns_exceeded() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/sample.csv"),
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"",
            "maxColumns",
            1);
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .satisfies(
            t ->
                assertThat(t)
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("ArrayIndexOutOfBoundsException - 1")
                    .hasMessageContaining(
                        "Please increase the value of the connector.csv.maxColumns setting")
                    .hasRootCauseInstanceOf(ArrayIndexOutOfBoundsException.class));
    connector.close();
  }

  @Test
  void should_error_on_empty_url() {
    CSVConnector connector = new CSVConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "url", null);
    assertThatThrownBy(() -> connector.configure(settings, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "A URL or URL file is mandatory when using the csv connector for LOAD. Please set connector.csv.url or connector.csv.urlfile and "
                + "try again. See settings.md or help for more information.");
  }

  @Test
  void should_throw_exception_when_recursive_not_boolean() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "recursive", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.recursive, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_header_not_boolean() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "header", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.header, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_skipRecords_not_number() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "skipRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.skipRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxRecords_not_number() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "maxRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.maxRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxConcurrentFiles_not_number() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "maxConcurrentFiles", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.maxConcurrentFiles, expecting positive integer or string in 'nC' syntax, got 'NotANumber'");
    connector.close();
  }

  @Test
  void should_throw_exception_when_encoding_not_valid() {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "encoding", "NotAnEncoding");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.encoding, expecting valid charset name, got 'NotAnEncoding'");
    connector.close();
  }

  @Test
  void should_throw_exception_when_delimiter_not_valid() {
    CSVConnector connector = new CSVConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "delimiter", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.delimiter, expecting single char, got ''");
    connector.close();
  }

  @Test
  void should_throw_exception_when_quote_not_valid() {
    CSVConnector connector = new CSVConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "quote", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.quote, expecting single char, got ''");
    connector.close();
  }

  @Test
  void should_throw_exception_when_escape_not_valid() {
    CSVConnector connector = new CSVConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "escape", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.escape, expecting single char, got ''");
    connector.close();
  }

  @Test
  void should_throw_exception_when_comment_not_valid() {
    CSVConnector connector = new CSVConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.csv", "comment", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.csv.comment, expecting single char, got ''");
    connector.close();
  }

  @Test()
  void should_error_when_compression_is_wrong() {
    CSVConnector connector = new CSVConnector();
    // empty string test
    Config settings1 =
        TestConfigUtils.createTestConfig("dsbulk.connector.csv", "compression", "\"abc\"");
    assertThrows(IllegalArgumentException.class, () -> connector.configure(settings1, false));
  }

  @Test
  void should_throw_IOE_when_read_wrong_compression() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            url("/sample.csv.gz"),
            "escape",
            "\"\\\"\"",
            "comment",
            "\"#\"",
            "compression",
            "\"bzip2\"");
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .hasRootCauseExactlyInstanceOf(IOException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining("Stream is not in the BZip2 format"));
    connector.close();
  }

  /** Test for DAT-427. */
  @Test
  void should_reject_header_with_empty_field() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/bad_header_empty.csv"));
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .satisfies(
            t -> {
              Throwable root = getRootCause(t.getCause());
              assertThat(root)
                  .isInstanceOf(IOException.class)
                  .hasMessageContaining(
                      "bad_header_empty.csv has invalid header: "
                          + "found empty field name at index 1; "
                          + "found empty field name at index 2");
            });
    connector.close();
  }

  /** Test for DAT-427. */
  @Test
  void should_reject_header_with_duplicate_field() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "url", url("/bad_header_duplicate.csv"));
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .satisfies(
            t -> {
              Throwable root = getRootCause(t.getCause());
              assertThat(root)
                  .isInstanceOf(IOException.class)
                  .hasMessageContaining(
                      "bad_header_duplicate.csv has invalid header: "
                          + "found duplicate field name at index 1; "
                          + "found duplicate field name at index 2");
            });
    connector.close();
  }

  private List<Record> createRecords() {
    ArrayList<Record> records = new ArrayList<>();
    Field[] fields =
        Arrays.stream(new String[] {"Year", "Make", "Model", "Description", "Price"})
            .map(DefaultMappedField::new)
            .toArray(Field[]::new);
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            "1997",
            "Ford",
            "E350",
            "  ac, abs, moon  ",
            "3000.00"));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            null,
            "4900.00"));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00"));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            null,
            "5000.00"));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            null,
            null,
            "Venture \"Extended Edition\"",
            null,
            "4900.00"));
    return records;
  }

  @Test
  void should_throw_if_passing_urlfile_parameter_for_write() {
    CSVConnector connector = new CSVConnector();

    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv", "urlfile", StringUtils.quoteJson(MULTIPLE_URLS_FILE));

    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The urlfile parameter is not supported for UNLOAD");
  }

  @Test
  void should_not_throw_and_log_if_passing_both_url_and_urlfile_parameter(
      @LogCapture(level = Level.DEBUG) LogInterceptor logs) {
    CSVConnector connector = new CSVConnector();

    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "urlfile",
            StringUtils.quoteJson(MULTIPLE_URLS_FILE),
            "url",
            StringUtils.quoteJson(MULTIPLE_URLS_FILE));

    assertDoesNotThrow(() -> connector.configure(settings, true));

    assertThat(logs.getLoggedMessages())
        .contains("You specified both URL and URL file. The URL file will take precedence.");
  }

  @Test
  void should_accept_multiple_urls() throws IOException, URISyntaxException {
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "urlfile",
            StringUtils.quoteJson(MULTIPLE_URLS_FILE),
            "recursive",
            false,
            "fileNamePattern",
            "\"**/part-*\"");
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(400);
    connector.close();
  }

  /** DAT-516: Always quote comment character when unloading */
  @Test
  void should_quote_comment_character() throws Exception {
    Path out = Files.createTempDirectory("test");
    CSVConnector connector = new CSVConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.csv",
            "url",
            StringUtils.quoteJson(out),
            "header",
            "false",
            "maxConcurrentFiles",
            1,
            "comment",
            "\"#\"");
    connector.configure(settings, false);
    connector.init();
    Flux.<Record>just(
            DefaultRecord.indexed("source", resource, IRRELEVANT_POSITION, "#shouldbequoted"))
        .transform(connector.write())
        .blockFirst();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual).hasSize(1).containsExactly("\"#shouldbequoted\"");
  }

  private static String url(String resource) {
    return StringUtils.quoteJson(CSVConnectorTest.class.getResource(resource));
  }

  private static Path path(@SuppressWarnings("SameParameterValue") String resource)
      throws URISyntaxException {
    return Paths.get(CSVConnectorTest.class.getResource(resource).toURI());
  }

  private static String rawURL(String resource) {
    return CSVConnectorTest.class.getResource(resource).toExternalForm();
  }
}
