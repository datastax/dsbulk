/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.json;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.createURLFile;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.readFile;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
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

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.io.CompressedIOUtils;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.commons.tests.utils.URLUtils;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
class JsonConnectorTest {

  private static final int IRRELEVANT_POSITION = -1;

  static {
    URLUtils.setURLFactoryIfNeeded();
    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {});
  }

  private final JsonNodeFactory factory = JsonNodeFactory.instance;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final URI resource = URI.create("file://file1.csv");

  private static Path MULTIPLE_URLS_FILE;

  @BeforeAll
  static void setup() throws IOException {
    MULTIPLE_URLS_FILE =
        createURLFile(
            Arrays.asList(
                rawURL("/part_1"), rawURL("/part_2"), rawURL("/root-custom/child/part-0003")));
  }

  @AfterAll
  static void cleanup() throws IOException {
    Files.delete(MULTIPLE_URLS_FILE);
  }

  @ParameterizedTest(name = "[{index}] read multi doc file {0} with compression {1}")
  @MethodSource
  @DisplayName("Should read multidoc file with given compression")
  void should_read_single_file_multi_doc(final String fileName, final String compression)
      throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/" + fileName),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}",
            "compression",
            quoteJson(compression));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_read_single_file_multi_doc() {
    return Stream.of(
        arguments("multi_doc.json", CompressedIOUtils.NONE_COMPRESSION),
        arguments("multi_doc.json.gz", CompressedIOUtils.GZIP_COMPRESSION),
        arguments("multi_doc.json.bz2", CompressedIOUtils.BZIP2_COMPRESSION),
        arguments("multi_doc.json.lz4", CompressedIOUtils.LZ4_COMPRESSION),
        arguments("multi_doc.json.snappy", CompressedIOUtils.SNAPPY_COMPRESSION),
        arguments("multi_doc.json.z", CompressedIOUtils.Z_COMPRESSION),
        arguments("multi_doc.json.br", CompressedIOUtils.BROTLI_COMPRESSION),
        arguments("multi_doc.json.lzma", CompressedIOUtils.LZMA_COMPRESSION),
        arguments("multi_doc.json.xz", CompressedIOUtils.XZ_COMPRESSION),
        arguments("multi_doc.json.zstd", CompressedIOUtils.ZSTD_COMPRESSION));
  }

  @Test
  void should_read_single_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/single_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}",
            "mode",
            "SINGLE_DOCUMENT");
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @Test
  void should_read_single_empty_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/empty.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "SINGLE_DOCUMENT");
    connector.configure(settings, true);
    connector.init();
    // should complete with 0 records.
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @Test
  void should_read_single_empty_file_multi_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/empty.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "MULTI_DOCUMENT");
    connector.configure(settings, true);
    connector.init();
    // should complete with 0 records.
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @Test
  void should_read_single_file_by_resource() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/multi_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}");
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.merge(connector.readByResource()).collectList().block();
    verifyRecords(actual);
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
      LoaderConfig settings = createTestConfig("dsbulk.connector.json", "encoding", "ISO-8859-1");
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(objectMapper.readTree(line));
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
      LoaderConfig settings = createTestConfig("dsbulk.connector.json", "encoding", "ISO-8859-1");
      connector.configure(settings, false);
      connector.init();
      Flux.<Record>just(
              DefaultRecord.indexed(
                  "source",
                  resource,
                  IRRELEVANT_POSITION,
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
  void should_read_all_resources_in_directory() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", url("/root"), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_by_resource() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", url("/root"), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_with_path() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Paths.get(getClass().getResource("/root").toURI());
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", quoteJson(rootPath), "recursive", false);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", url("/root"), "recursive", true);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively_by_resource() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", url("/root"), "recursive", true);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_scan_directory_recursively_with_custom_file_name_format() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
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
    JsonConnector connector = new JsonConnector();
    Path rootPath = Files.createTempDirectory("empty");
    try {
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
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
      deleteDirectory(rootPath);
    }
  }

  @Test
  void should_warn_when_no_files_matched(@LogCapture LogInterceptor logs) throws Exception {
    JsonConnector connector = new JsonConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
              "recursive",
              true,
              "fileNamePattern",
              "\"**/part-*\"");
      connector.configure(settings, true);
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
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(rootPath),
              "recursive",
              true,
              "compression",
              quoteJson(compression));

      connector.configure(settings, true);
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
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1);
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
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
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "mode",
              "SINGLE_DOCUMENT");
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
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
      LoaderConfig settings =
          createTestConfig(
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
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
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
      LoaderConfig settings =
          createTestConfig(
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
      LoaderConfig settings =
          createTestConfig(
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
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.json"));
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "[",
              "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Price\":4900.0},",
              "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0},",
              "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Price\":5000.0},",
              "{\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Price\":4900.0}",
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
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              quoteJson(out),
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
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "url", quoteJson(out));
    connector.configure(settings, false);
    connector.init();
    connector.counter.set(999);
    assertThat(connector.getOrCreateDestinationURL().getPath()).endsWith("output-001000.json");
    connector.counter.set(999_999);
    assertThat(connector.getOrCreateDestinationURL().getPath()).endsWith("output-1000000.json");
  }

  @Test
  void should_roll_file_when_max_records_reached() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    try {
      String escapedPath = quoteJson(out);
      LoaderConfig settings =
          createTestConfig(
              "dsbulk.connector.json",
              "url",
              escapedPath,
              "escape",
              "\"\\\"\"",
              "maxConcurrentFiles",
              1,
              "maxRecords",
              3);
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
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
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "skipRecords", 10);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(450);
    connector.close();
  }

  @Test
  void should_skip_records2() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "skipRecords", 150);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(0);
    connector.close();
  }

  @Test
  void should_honor_max_records() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "maxRecords", 10);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(50);
    connector.close();
  }

  @Test
  void should_honor_max_records2() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json", "url", url("/root"), "recursive", true, "maxRecords", 1);
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(5);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
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
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/root/ip-by-country-sample1.json"),
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
            "{\"beginning IP Address\":\"212.63.180.20\",\"ending IP Address\":\"212.63.180.23\",\"beginning IP Number\":3560944660,\"ending IP Number\":3560944663,\"ISO 3166 Country Code\":\"MZ\",\"Country Name\":\"Mozambique\"}");
    connector.close();
  }

  @Test
  void should_error_on_empty_url() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "url", "\"\"");
    assertThatThrownBy(() -> connector.configure(settings, true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "A URL or URL file is mandatory when using the json connector for LOAD. Please set connector.json.url or connector.json.urlfile and "
                + "try again. See settings.md or help for more information.");
  }

  @Test
  void should_throw_if_passing_urlfile_parameter_for_write() {
    JsonConnector connector = new JsonConnector();

    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "urlfile", quoteJson(MULTIPLE_URLS_FILE));

    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("The urlfile parameter is not supported for UNLOAD");
  }

  @Test
  void should_not_throw_and_log_if_passing_both_url_and_urlfile_parameter(
      @LogCapture(level = Level.DEBUG) LogInterceptor logs) {
    JsonConnector connector = new JsonConnector();

    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "urlfile",
            quoteJson(MULTIPLE_URLS_FILE),
            "url",
            quoteJson(MULTIPLE_URLS_FILE));

    assertDoesNotThrow(() -> connector.configure(settings, true));

    assertThat(logs.getLoggedMessages())
        .contains("You specified both URL and URL file. The URL file will take precedence.");
  }

  @Test
  void should_accept_multiple_urls() throws IOException, URISyntaxException {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "urlfile",
            quoteJson(MULTIPLE_URLS_FILE),
            "recursive",
            false,
            "fileNamePattern",
            "\"**/part-*\"");
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(400);
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
      LoaderConfig settings =
          createTestConfig("dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false);
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
      LoaderConfig settings =
          createTestConfig("dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 1);
      connector.configure(settings, false);
      connector.init();
      Path file = out.resolve("output-000001.json");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      assertThatThrownBy(
              () -> Flux.fromIterable(createRecords()).transform(connector.write()).blockLast())
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
      LoaderConfig settings =
          createTestConfig("dsbulk.connector.json", "url", quoteJson(out), "maxConcurrentFiles", 2);
      connector.configure(settings, false);
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
      deleteDirectory(out);
    }
  }

  @Test
  void should_report_wrong_document_mode() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            url("/single_doc.json"),
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "mode",
            "MULTI_DOCUMENT");
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.from(connector.read()).collectList().block())
        .hasRootCauseExactlyInstanceOf(JsonParseException.class)
        .satisfies(
            t ->
                assertThat(getRootCause(t))
                    .hasMessageContaining(
                        "Expecting START_OBJECT, got START_ARRAY. Did you forget to set connector.json.mode to SINGLE_DOCUMENT?"));
    connector.close();
  }

  @Test
  void should_read_from_http_url(@Wiremock WireMockServer server) throws Exception {
    server.givenThat(
        any(urlPathEqualTo("/file.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/json")
                    .withBody(readFile(path("/single_doc.json")))));
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
            "dsbulk.connector.json",
            "url",
            String.format("\"%s/file.json\"", server.baseUrl()),
            "mode",
            "SINGLE_DOCUMENT",
            "parserFeatures",
            "{ALLOW_COMMENTS:true}",
            "deserializationFeatures",
            "{USE_BIG_DECIMAL_FOR_FLOATS : false}");
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @Test
  void should_not_write_to_http_url() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "url", "\"http://localhost:1234/file.json\"");
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
                        "HTTP/HTTPS protocols cannot be used for output: http://localhost:1234/file.json"));
    connector.close();
  }

  @Test
  void should_throw_exception_when_recursive_not_boolean() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "recursive", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.recursive, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_prettyPrint_not_boolean() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "prettyPrint", "NotABoolean");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.prettyPrint, expecting BOOLEAN, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_skipRecords_not_number() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "skipRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.skipRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxRecords_not_number() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "maxRecords", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.maxRecords, expecting NUMBER, got STRING");
    connector.close();
  }

  @Test
  void should_throw_exception_when_maxConcurrentFiles_not_number() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig("dsbulk.connector.json", "maxConcurrentFiles", "NotANumber");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.maxConcurrentFiles, expecting integer or string in 'nC' syntax, got 'NotANumber'");
    connector.close();
  }

  @Test
  void should_throw_exception_when_encoding_not_valid() {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings = createTestConfig("dsbulk.connector.json", "encoding", "NotAnEncoding");
    assertThatThrownBy(() -> connector.configure(settings, false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.connector.json.encoding, expecting valid charset name, got 'NotAnEncoding'");
    connector.close();
  }

  @Test()
  void should_throw_exception_when_compression_is_wrong() {
    JsonConnector connector = new JsonConnector();
    // empty string test
    LoaderConfig settings1 = createTestConfig("dsbulk.connector.json", "compression", "abc");
    assertThrows(BulkConfigurationException.class, () -> connector.configure(settings1, false));
  }

  @Test
  void should_throw_exception_when_doc_compression_is_wrong() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        createTestConfig(
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

  private void verifyRecords(List<Record> actual) {
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsOnly(
            factory.numberNode(1997),
            factory.textNode("Ford"),
            factory.textNode("E350"),
            factory.textNode("ac, abs, moon"),
            factory.numberNode(3000.00d));
    assertThat(actual.get(1).values())
        .containsOnly(
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition\""),
            factory.nullNode(),
            factory.numberNode(4900.00d));
    assertThat(actual.get(2).values())
        .containsOnly(
            factory.numberNode(1996),
            factory.textNode("Jeep"),
            factory.textNode("Grand Cherokee"),
            factory.textNode("MUST SELL!\nair, moon roof, loaded"),
            factory.numberNode(4799.00d));
    assertThat(actual.get(3).values())
        .containsOnly(
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition, Very Large\""),
            factory.nullNode(),
            factory.numberNode(5000.00d));
    assertThat(actual.get(4).values())
        .containsOnly(
            factory.nullNode(),
            factory.nullNode(),
            factory.textNode("Venture \"Extended Edition\""),
            factory.nullNode(),
            factory.numberNode(4900.00d));
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
            factory.numberNode(1997),
            factory.textNode("Ford"),
            factory.textNode("E350"),
            factory.textNode("ac, abs, moon"),
            factory.numberNode(3000.00d)));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition\""),
            null,
            factory.numberNode(4900.00d)));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            factory.numberNode(1996),
            factory.textNode("Jeep"),
            factory.textNode("Grand Cherokee"),
            factory.textNode("MUST SELL!\nair, moon roof, loaded"),
            factory.numberNode(4799.00d)));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition, Very Large\""),
            null,
            factory.numberNode(5000.00d)));
    records.add(
        DefaultRecord.mapped(
            "source",
            resource,
            IRRELEVANT_POSITION,
            fields,
            null,
            null,
            factory.textNode("Venture \"Extended Edition\""),
            null,
            factory.numberNode(4900.00d)));
    return records;
  }

  private static String url(String resource) {
    return quoteJson(JsonConnectorTest.class.getResource(resource));
  }

  private static String rawURL(String resource) {
    return JsonConnectorTest.class.getResource(resource).toExternalForm();
  }

  private static Path path(@SuppressWarnings("SameParameterValue") String resource)
      throws URISyntaxException {
    return Paths.get(JsonConnectorTest.class.getResource(resource).toURI());
  }
}
