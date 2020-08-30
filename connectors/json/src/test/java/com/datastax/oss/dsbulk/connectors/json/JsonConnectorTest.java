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
package com.datastax.oss.dsbulk.connectors.json;

import static com.datastax.oss.dsbulk.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readFile;
import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Throwables.getRootCause;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.io.CompressedIOUtils;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.datastax.oss.dsbulk.url.BulkLoaderURLStreamHandlerFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import ru.lanwen.wiremock.ext.WiremockResolver;
import ru.lanwen.wiremock.ext.WiremockResolver.Wiremock;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(WiremockResolver.class)
class JsonConnectorTest {

  static {
    BulkLoaderURLStreamHandlerFactory.install();
    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {});
  }

  private final JsonNodeFactory factory = JsonNodeFactory.instance;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final URI resource = URI.create("file://file1.csv");

  private static Path multipleUrlsFile;
  private static Path urlsFileWithStdin;

  @BeforeAll
  static void setupURLFiles() throws IOException {
    multipleUrlsFile =
        FileUtils.createURLFile(
            rawURL("/part_1"), rawURL("/part_2"), rawURL("/root-custom/child/part-0003"));
    urlsFileWithStdin =
        FileUtils.createURLFile(
            rawURL("/part_1"),
            rawURL("/part_2"),
            rawURL("/root-custom/child/part-0003"),
            new URL("std:/"));
  }

  @AfterAll
  static void cleanupURLFiles() throws IOException {
    Files.delete(multipleUrlsFile);
    Files.delete(urlsFileWithStdin);
  }

  @ParameterizedTest(name = "[{index}] read multi doc file {0} with compression {1} (sources: {2})")
  @MethodSource
  @DisplayName("Should read multidoc file with given compression")
  void should_read_single_file_multi_doc(
      String fileName, String compression, boolean retainRecordSources) throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/" + fileName),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}",
            "compression",
            quoteJson(compression));
    connector.configure(settings, true, retainRecordSources);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    verifyRecords(actual, retainRecordSources, rawURL("/" + fileName).toURI());
    connector.close();
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_read_single_file_multi_doc() {
    return Stream.of(
        arguments("multi_doc.json", CompressedIOUtils.NONE_COMPRESSION, true),
        arguments("multi_doc.json.gz", CompressedIOUtils.GZIP_COMPRESSION, true),
        arguments("multi_doc.json.bz2", CompressedIOUtils.BZIP2_COMPRESSION, true),
        arguments("multi_doc.json.lz4", CompressedIOUtils.LZ4_COMPRESSION, true),
        arguments("multi_doc.json.snappy", CompressedIOUtils.SNAPPY_COMPRESSION, true),
        arguments("multi_doc.json.z", CompressedIOUtils.Z_COMPRESSION, true),
        arguments("multi_doc.json.br", CompressedIOUtils.BROTLI_COMPRESSION, true),
        arguments("multi_doc.json.lzma", CompressedIOUtils.LZMA_COMPRESSION, true),
        arguments("multi_doc.json.xz", CompressedIOUtils.XZ_COMPRESSION, true),
        arguments("multi_doc.json.zstd", CompressedIOUtils.ZSTD_COMPRESSION, true),
        arguments("multi_doc.json", CompressedIOUtils.NONE_COMPRESSION, false),
        arguments("multi_doc.json.gz", CompressedIOUtils.GZIP_COMPRESSION, false),
        arguments("multi_doc.json.bz2", CompressedIOUtils.BZIP2_COMPRESSION, false),
        arguments("multi_doc.json.lz4", CompressedIOUtils.LZ4_COMPRESSION, false),
        arguments("multi_doc.json.snappy", CompressedIOUtils.SNAPPY_COMPRESSION, false),
        arguments("multi_doc.json.z", CompressedIOUtils.Z_COMPRESSION, false),
        arguments("multi_doc.json.br", CompressedIOUtils.BROTLI_COMPRESSION, false),
        arguments("multi_doc.json.lzma", CompressedIOUtils.LZMA_COMPRESSION, false),
        arguments("multi_doc.json.xz", CompressedIOUtils.XZ_COMPRESSION, false),
        arguments("multi_doc.json.zstd", CompressedIOUtils.ZSTD_COMPRESSION, false));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_read_single_file_single_doc(boolean retainRecordSources) throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/single_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}",
            "mode",
            "SINGLE_DOCUMENT");
    connector.configure(settings, true, retainRecordSources);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    verifyRecords(actual, retainRecordSources, rawURL("/single_doc.json").toURI());
    connector.close();
  }

  @Test
  void should_read_single_empty_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/empty.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "SINGLE_DOCUMENT");
    connector.configure(settings, true, true);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    // should complete with 0 records.
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @Test
  void should_read_single_empty_file_multi_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/empty.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "MULTI_DOCUMENT");
    connector.configure(settings, true, true);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    // should complete with 0 records.
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_read_single_file_multi_doc(boolean retainRecordSources) throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/multi_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}");
    connector.configure(settings, true, retainRecordSources);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    verifyRecords(actual, retainRecordSources, rawURL("/multi_doc.json").toURI());
    connector.close();
  }

  @Test
  void should_read_from_stdin_with_special_encoding() throws Exception {
    InputStream stdin = System.in;
    try {
      String line = "{ \"fóô\" : \"bàr\", \"qïx\" : null }\n";
      InputStream is = new ByteArrayInputStream(line.getBytes(ISO_8859_1));
      System.setIn(is);
      JsonConnector connector = new JsonConnector();
      Config settings =
          TestConfigUtils.createTestConfig("dsbulk.connector.json", "encoding", "ISO-8859-1");
      connector.configure(settings, true, true);
      connector.init();
      assertThat(connector.readConcurrency()).isOne();
      assertThat(
              ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
          .isFalse();
      List<Record> actual = Flux.merge(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(objectMapper.readTree(line));
      assertThat(actual.get(0).getResource()).isEqualTo(URI.create("std:/"));
      assertThat(actual.get(0).getPosition()).isEqualTo(1L);
      assertThat(actual.get(0).getFieldValue(new DefaultMappedField("fóô")))
          .isEqualTo(factory.textNode("bàr"));
      assertThat(actual.get(0).getFieldValue(new DefaultMappedField("qïx")))
          .isEqualTo(factory.nullNode());
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
      JsonConnector connector = new JsonConnector();
      Config settings =
          TestConfigUtils.createTestConfig("dsbulk.connector.json", "encoding", "ISO-8859-1");
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      assertThat(
              ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
          .isFalse();
      Flux.<Record>just(
              DefaultRecord.indexed(
                  "source",
                  resource,
                  1,
                  factory.textNode("fóô"),
                  factory.textNode("bàr"),
                  factory.textNode("qïx")))
          .transform(connector.write())
          .blockLast();
      connector.close();
      assertThat(new String(baos.toByteArray(), ISO_8859_1))
          .isEqualTo("{\"0\":\"fóô\",\"1\":\"bàr\",\"2\":\"qïx\"}" + System.lineSeparator());
    } finally {
      System.setOut(stdout);
    }
  }

  @Test
  void should_allow_data_size_sampling_when_not_reading_from_stdin_single_url() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "url", url("/single_doc.json"));
    connector.configure(settings, true, true);
    connector.init();
    assertThat(ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
        .isTrue();
    assertThat(connector.supports(CommonConnectorFeature.DATA_SIZE_SAMPLING)).isTrue();
  }

  @Test
  void should_allow_data_size_sampling_when_urlfile_does_not_contain_stdin() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "urlfile", quoteJson(multipleUrlsFile));
    connector.configure(settings, true, true);
    connector.init();
    assertThat(ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
        .isTrue();
    assertThat(connector.supports(CommonConnectorFeature.DATA_SIZE_SAMPLING)).isTrue();
  }

  @Test
  void should_disallow_data_size_sampling_when_reading_from_stdin_single_url() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.json", "url", "-");
    connector.configure(settings, true, true);
    connector.init();
    assertThat(ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
        .isFalse();
    assertThat(connector.supports(CommonConnectorFeature.DATA_SIZE_SAMPLING)).isFalse();
  }

  @Test
  void should_disallow_data_size_sampling_when_reading_from_stdin_urlfile() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "urlfile", quoteJson(urlsFileWithStdin));
    connector.configure(settings, true, true);
    connector.init();
    assertThat(ReflectionUtils.invokeMethod("isDataSizeSamplingAvailable", connector, Boolean.TYPE))
        .isFalse();
    assertThat(connector.supports(CommonConnectorFeature.DATA_SIZE_SAMPLING)).isFalse();
  }

  @Test
  void should_read_all_resources_in_directory() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root"),
            "recursive",
            false,
            "maxConcurrentFiles",
            4);
    connector.configure(settings, true, true);
    connector.init();
    // there are only 3 resources to read
    assertThat(connector.readConcurrency()).isEqualTo(3);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_with_path() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Paths.get(getClass().getResource("/root").toURI());
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            quoteJson(rootPath),
            "recursive",
            false,
            "maxConcurrentFiles",
            4);
    connector.configure(settings, true, true);
    connector.init();
    // there are only 3 resources to read
    assertThat(connector.readConcurrency()).isEqualTo(3);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root"),
            "recursive",
            true,
            "maxConcurrentFiles",
            4);
    connector.configure(settings, true, true);
    connector.init();
    // 5 resources to read, but maxConcurrentFiles is 4
    assertThat(connector.readConcurrency()).isEqualTo(4);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_scan_directory_recursively_with_custom_file_name_format() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root-custom"),
            "recursive",
            true,
            "fileNamePattern",
            "\"**/part-*\"");
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_warn_when_directory_empty(@LogCapture LogInterceptor logs) throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Files.createTempDirectory("empty");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
              "recursive",
              true,
              "fileNamePattern",
              "\"**/part-*\"");
      connector.configure(settings, true, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(String.format("Directory %s has no readable files.", rootPath));
      connector.close();
    } finally {
      deleteDirectory(rootPath);
    }
  }

  @Test
  void should_warn_when_no_files_matched(@LogCapture LogInterceptor logs) throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
              "recursive",
              true,
              "fileNamePattern",
              "\"**/part-*\"");
      connector.configure(settings, true, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(
              String.format(
                  "No files in directory %s matched the connector.json.fileNamePattern of \"**/part-*\".",
                  rootPath));
      connector.close();
    } finally {
      deleteDirectory(rootPath);
    }
  }

  @ParameterizedTest(name = "[{index}] Should get correct message when compression {0} is used")
  @MethodSource
  @DisplayName(
      "Should get correct exception message when there are no matching files with compression enabled")
  void should_warn_when_no_files_matched_and_compression_enabled(
      String compression, @LogCapture LogInterceptor logs) throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
              "recursive",
              true,
              "compression",
              quoteJson(compression));

      connector.configure(settings, true, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(
              String.format(
                  "No files in directory %s matched the connector.json.fileNamePattern of \"**/*.json%s\".",
                  rootPath, CompressedIOUtils.getCompressionSuffix(compression)));
      connector.close();
    } finally {
      deleteDirectory(rootPath);
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
  void should_write_single_file_multi_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1);
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.json"));
      assertThat(actual).hasSize(5);
      assertThat(actual)
          .containsExactly(
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "mode",
              "SINGLE_DOCUMENT");
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.json"));
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "[",
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0},",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0},",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "]");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_single_doc_compressed_gzip() throws Exception {
    JsonConnector connector = new JsonConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "compression",
              "gzip",
              "maxConcurrentFiles",
              1,
              "mode",
              "SINGLE_DOCUMENT");
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
      connector.close();
      Path outPath = out.resolve("output-000001.json.gz");
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  new GZIPInputStream(Files.newInputStream(outPath)), Charsets.UTF_8));
      List<String> actual = reader.lines().collect(Collectors.toList());
      reader.close();
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "[",
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0},",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0},",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "]");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_single_doc_compressed_gzip_custom_file_format() throws Exception {
    JsonConnector connector = new JsonConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "compression",
              "gzip",
              "fileNameFormat",
              "file%d",
              "maxConcurrentFiles",
              1,
              "mode",
              "SINGLE_DOCUMENT");
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
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
              "[",
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0},",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0},",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "]");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_single_file_single_doc_and_skip_nulls() throws Exception {
    JsonConnector connector = new JsonConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "mode",
              "SINGLE_DOCUMENT",
              "serializationStrategy",
              "NON_NULL");
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.json"));
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "[",
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0},",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0},",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "]");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_multiple_files() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      int maxConcurrentFiles = 4;
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              maxConcurrentFiles);
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isEqualTo(maxConcurrentFiles);
      // repeat the records 1000 times to fully exercise multiple file writing
      Scheduler scheduler = Schedulers.newParallel("workflow");
      Function<Publisher<Record>, Publisher<Record>> write = connector.write();
      Flux.range(0, 1000)
          .flatMap(
              i ->
                  Flux.fromIterable(createRecords(false, resource))
                      .transform(write)
                      .subscribeOn(scheduler),
              maxConcurrentFiles)
          .blockLast();
      connector.close();
      List<String> actual =
          FileUtils.readAllLinesInDirectoryAsStream(out)
              .sorted()
              .distinct()
              .collect(Collectors.toList());
      assertThat(actual)
          .containsOnly(
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
    } finally {
      deleteDirectory(out);
    }
  }

  // Test for DAT-443
  @Test
  void should_generate_file_name() throws Exception {
    Path out = Files.createTempDirectory("test");
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "url", quoteJson(out));
    connector.configure(settings, false, true);
    connector.init();
    AtomicInteger counter =
        (AtomicInteger) ReflectionUtils.getInternalState(connector, "fileCounter");
    Method getOrCreateDestinationURL =
        ReflectionUtils.locateMethod("getOrCreateDestinationURL", JsonConnector.class, 0);
    counter.set(999);
    URL nextFile = ReflectionUtils.invokeMethod(getOrCreateDestinationURL, connector, URL.class);
    assertThat(nextFile.getPath()).endsWith("output-001000.json");
    counter.set(999_999);
    nextFile = ReflectionUtils.invokeMethod(getOrCreateDestinationURL, connector, URL.class);
    assertThat(nextFile.getPath()).endsWith("output-1000000.json");
  }

  @Test
  void should_roll_file_when_max_records_reached() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      String escapedPath = quoteJson(out);
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json",
              "url",
              escapedPath,
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "maxRecords",
              3);
      connector.configure(settings, false, true);
      connector.init();
      assertThat(connector.writeConcurrency()).isOne();
      Flux.fromIterable(createRecords(false, resource)).transform(connector.write()).blockLast();
      connector.close();
      List<String> json1 = Files.readAllLines(out.resolve("output-000001.json"));
      List<String> json2 = Files.readAllLines(out.resolve("output-000002.json"));
      assertThat(json1).hasSize(3);
      assertThat(json2).hasSize(2);
      assertThat(json1)
          .containsExactly(
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}");
      assertThat(json2)
          .containsExactly(
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
              "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_skip_records() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "skipRecords", 10);
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(450);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(450);
    connector.close();
  }

  @Test
  void should_skip_records2() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "skipRecords", 150);
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(0);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(0);
    connector.close();
  }

  @Test
  void should_honor_max_records() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "maxRecords", 10);
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(50);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(50);
    connector.close();
  }

  @Test
  void should_honor_max_records2() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "maxRecords", 1);
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(5);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(5);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root"),
            "recursive",
            true,
            "skipRecords",
            95,
            "maxRecords",
            10);
    connector.configure(settings, true, true);
    connector.init();
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(25);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records2() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root/ip-by-country-sample1.json"),
            "skipRecords",
            10,
            "maxRecords",
            1);
    connector.configure(settings, true, true);
    connector.init();
    List<Record> records = Flux.merge(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getSource().toString().trim())
        .isEqualTo(
            "{\"beginning IP Address\":\"212.63.180.20\",\"ending IP Address\":\"212.63.180.23\",\"beginning IP Number\":3560944660,\"ending IP Number\":3560944663,\"ISO 3166 Country Code\":\"MZ\",\"Country Name\":\"Mozambique\"}");
    connector.close();
  }

  @Test
  void should_error_on_empty_url() {
    JsonConnector connector = new JsonConnector();
    Config settings = TestConfigUtils.createTestConfig("dsbulk.connector.json", "url", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, true, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "A URL or URL file is mandatory when using the json connector for LOAD. Please set connector.json.url or connector.json.urlfile and "
                + "try again. See settings.md or help for more information.");
  }

  @Test
  void should_throw_if_passing_urlfile_parameter_for_write() {
    JsonConnector connector = new JsonConnector();

    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "urlfile", quoteJson(multipleUrlsFile));

    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The urlfile parameter is not supported for UNLOAD");
  }

  @Test
  void should_not_throw_and_log_if_passing_both_url_and_urlfile_parameter(
      @LogCapture(level = Level.DEBUG) LogInterceptor logs) {
    JsonConnector connector = new JsonConnector();

    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "urlfile",
            quoteJson(multipleUrlsFile),
            "url",
            quoteJson(multipleUrlsFile));

    assertDoesNotThrow(() -> connector.configure(settings, true, true));

    assertThat(logs.getLoggedMessages())
        .contains("You specified both URL and URL file. The URL file will take precedence.");
  }

  @Test
  void should_accept_multiple_urls() throws IOException, URISyntaxException {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "urlfile",
            quoteJson(multipleUrlsFile),
            "recursive",
            false,
            "fileNamePattern",
            "\"**/part-*\"",
            "maxConcurrentFiles",
            8);
    connector.configure(settings, true, true);
    connector.init();
    // maxConcurrentFiles 8 but only 4 files to read
    assertThat(connector.readConcurrency()).isEqualTo(4);
    assertThat(Flux.merge(connector.read()).count().block()).isEqualTo(400);
    connector.close();
  }

  @Test
  void should_error_when_directory_is_not_empty() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Path file = out.resolve("output-000001.json");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false, true);
      assertThrows(IllegalArgumentException.class, connector::init);
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_abort_write_single_file_when_io_error() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false, true);
      connector.init();
      Path file = out.resolve("output-000001.json");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      assertThatThrownBy(
              () ->
                  Flux.fromIterable(createRecords(false, resource))
                      .transform(connector.write())
                      .blockLast())
          .hasRootCauseExactlyInstanceOf(FileAlreadyExistsException.class);
      connector.close();
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_abort_write_multiple_files_when_io_error() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Config settings =
          TestConfigUtils.createTestConfig(
              "dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 2);
      connector.configure(settings, false, true);
      connector.init();
      Path file1 = out.resolve("output-000001.json");
      Path file2 = out.resolve("output-000002.json");
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
                  Flux.fromIterable(createRecords(false, resource))
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
      deleteDirectory(out);
    }
  }

  @Test
  void should_report_wrong_document_mode() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/single_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "MULTI_DOCUMENT");
    connector.configure(settings, true, true);
    connector.init();
    assertThatThrownBy(() -> Flux.merge(connector.read()).collectList().block())
        .hasRootCauseExactlyInstanceOf(JsonParseException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining(
                        "Expecting START_OBJECT, got START_ARRAY. Did you forget to set connector.json.mode to SINGLE_DOCUMENT?"));
    connector.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void should_read_from_http_url(boolean retainRecordSources, @Wiremock WireMockServer server)
      throws Exception {
    server.givenThat(
        any(urlPathEqualTo("/file.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/json")
                    .withBody(readFile(path("/single_doc.json")))));
    JsonConnector connector = new JsonConnector();
    String url = String.format("%s/file.json", server.baseUrl());
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            quoteJson(url),
            "mode",
            "SINGLE_DOCUMENT",
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}");
    connector.configure(settings, true, retainRecordSources);
    connector.init();
    assertThat(connector.readConcurrency()).isOne();
    List<Record> actual = Flux.merge(connector.read()).collectList().block();
    verifyRecords(actual, retainRecordSources, new URI(url));
    connector.close();
  }

  @Test
  void should_not_write_to_http_url() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "url", "\"http://localhost:1234/file.json\"");
    connector.configure(settings, false, true);
    connector.init();
    assertThatThrownBy(
            () ->
                Flux.fromIterable(createRecords(false, resource))
                    .transform(connector.write())
                    .blockLast())
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining(
                        "HTTP/HTTPS protocols cannot be used for output: http://localhost:1234/file.json"));
    connector.close();
  }

  @Test
  void should_throw_exception_when_recursive_not_boolean() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "recursive", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.recursive, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_prettyPrint_not_boolean() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "prettyPrint", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.prettyPrint, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_skipRecords_not_number() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "skipRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.skipRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxRecords_not_number() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "maxRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.maxRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxConcurrentFiles_not_number() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json", "maxConcurrentFiles", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.maxConcurrentFiles, expecting positive integer or string in 'nC' syntax, got 'NotANumber'");
    connector.close();
  }

  @Test
  void should_accept_when_maxConcurrentFiles_AUTO() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "maxConcurrentFiles", "AUTO");
    connector.configure(settings, false, true);
    assertThat(ReflectionUtils.getInternalState(connector, "maxConcurrentFiles"))
        .isEqualTo(ConfigUtils.resolveThreads("0.5C"));
    connector.configure(settings, true, true);
    assertThat(ReflectionUtils.getInternalState(connector, "maxConcurrentFiles"))
        .isEqualTo(ConfigUtils.resolveThreads("1C"));
    connector.close();
  }

  @Test
  void should_throw_exception_when_encoding_not_valid() {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "encoding", "NotAnEncoding");
    assertThatThrownBy(() -> connector.configure(settings, false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.encoding, expecting valid charset name, got 'NotAnEncoding'");
    connector.close();
  }

  @Test()
  void should_throw_exception_when_compression_is_wrong() {
    JsonConnector connector = new JsonConnector();
    // empty string test
    Config settings1 =
        TestConfigUtils.createTestConfig("dsbulk.connector.json", "compression", "abc");
    assertThrows(IllegalArgumentException.class, () -> connector.configure(settings1, false, true));
  }

  @Test
  void should_throw_exception_when_doc_compression_is_wrong() throws Exception {
    JsonConnector connector = new JsonConnector();
    Config settings =
        TestConfigUtils.createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/multi_doc.json.gz"),
            "compression",
            "\"gzip\"",
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}",
            "compression",
            "bzip2");
    connector.configure(settings, true, true);
    connector.init();
    assertThatThrownBy(() -> Flux.merge(connector.read()).collectList().block())
        .hasRootCauseExactlyInstanceOf(IOException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining("Stream is not in the BZip2 format"));
    connector.close();
  }

  private void verifyRecords(List<Record> actual, boolean retainRecordSources, URI resource) {
    List<Record> expected = createRecords(retainRecordSources, resource);
    assertThat(actual).isEqualTo(expected);
  }

  private List<Record> createRecords(boolean retainRecordSources, URI resource) {
    ArrayList<Record> records = new ArrayList<>();
    Field[] fields = new Field[5];
    fields[0] = new DefaultMappedField("Year");
    fields[1] = new DefaultMappedField("Make");
    fields[2] = new DefaultMappedField("Model");
    fields[3] = new DefaultMappedField("Description");
    fields[4] = new DefaultMappedField("Price");
    JsonNode source1;
    JsonNode source2;
    JsonNode source3;
    JsonNode source4;
    JsonNode source5;
    try {
      source1 =
          objectMapper.readTree(
              "{"
                  + "\"Year\": 1997,\n"
                  + "\"Make\": \"Ford\",\n"
                  + "\"Model\": \"E350\",\n"
                  + "\"Description\": \"ac, abs, moon\",\n"
                  + "\"Price\": 3000.0\n"
                  + "}");
      source2 =
          objectMapper.readTree(
              "{\n"
                  + "\"Year\": 1999,\n"
                  + "\"Make\": \"Chevy\",\n"
                  + "\"Model\": \"Venture \\\"Extended Edition\\\"\",\n"
                  + "\"Description\": null,\n"
                  + "\"Price\": 4900.0\n"
                  + "}");
      source3 =
          objectMapper.readTree(
              "{\n"
                  + "\"Year\": 1996,\n"
                  + "\"Make\": \"Jeep\",\n"
                  + "\"Model\": \"Grand Cherokee\",\n"
                  + "\"Description\": \"MUST SELL!\\nair, moon roof, loaded\",\n"
                  + "\"Price\": 4799.0\n"
                  + "}");
      source4 =
          objectMapper.readTree(
              "{\n"
                  + "\"Year\": 1999,\n"
                  + "\"Make\": \"Chevy\",\n"
                  + "\"Model\": \"Venture \\\"Extended Edition, Very Large\\\"\",\n"
                  + "\"Description\": null,\n"
                  + "\"Price\": 5000.0\n"
                  + "}");
      source5 =
          objectMapper.readTree(
              "{\n"
                  + "\"Year\": null,\n"
                  + "\"Make\": null,\n"
                  + "\"Model\": \"Venture \\\"Extended Edition\\\"\",\n"
                  + "\"Description\": null,\n"
                  + "\"Price\": 4900.0\n"
                  + "}");
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
    records.add(
        DefaultRecord.mapped(
            retainRecordSources ? source1 : null,
            resource,
            1,
            fields,
            source1.get("Year"),
            source1.get("Make"),
            source1.get("Model"),
            source1.get("Description"),
            source1.get("Price")));
    records.add(
        DefaultRecord.mapped(
            retainRecordSources ? source2 : null,
            resource,
            2,
            fields,
            source2.get("Year"),
            source2.get("Make"),
            source2.get("Model"),
            source2.get("Description"),
            source2.get("Price")));
    records.add(
        DefaultRecord.mapped(
            retainRecordSources ? source3 : null,
            resource,
            3,
            fields,
            source3.get("Year"),
            source3.get("Make"),
            source3.get("Model"),
            source3.get("Description"),
            source3.get("Price")));
    records.add(
        DefaultRecord.mapped(
            retainRecordSources ? source4 : null,
            resource,
            4,
            fields,
            source4.get("Year"),
            source4.get("Make"),
            source4.get("Model"),
            source4.get("Description"),
            source4.get("Price")));
    records.add(
        DefaultRecord.mapped(
            retainRecordSources ? source5 : null,
            resource,
            5,
            fields,
            source5.get("Year"),
            source5.get("Make"),
            source5.get("Model"),
            source5.get("Description"),
            source5.get("Price")));
    return records;
  }

  private static String url(String resource) {
    return quoteJson(rawURL(resource));
  }

  private static URL rawURL(String resource) {
    return JsonConnectorTest.class.getResource(resource);
  }

  private static Path path(@SuppressWarnings("SameParameterValue") String resource)
      throws URISyntaxException {
    return Paths.get(JsonConnectorTest.class.getResource(resource).toURI());
  }
}
