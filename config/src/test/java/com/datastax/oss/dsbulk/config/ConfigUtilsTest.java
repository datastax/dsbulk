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
package com.datastax.oss.dsbulk.config;

import static com.typesafe.config.ConfigValueType.BOOLEAN;
import static com.typesafe.config.ConfigValueType.LIST;
import static com.typesafe.config.ConfigValueType.NULL;
import static com.typesafe.config.ConfigValueType.NUMBER;
import static com.typesafe.config.ConfigValueType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.url.BulkLoaderURLStreamHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigException.BadBean;
import com.typesafe.config.ConfigFactory;
import java.io.Console;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigUtilsTest {

  static {
    BulkLoaderURLStreamHandlerFactory.install();
  }

  @Test
  void should_create_reference_config() {
    Config referenceConfig = ConfigUtils.createReferenceConfig();
    assertReferenceConfig(referenceConfig);
  }

  @Test
  void should_create_application_config() {
    Config applicationConfig = ConfigUtils.createApplicationConfig(null);
    assertReferenceConfig(applicationConfig);
    // should read application.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInApplication")).isTrue();
    assertThat(applicationConfig.getValue("dsbulk.definedInApplication").origin().description())
        .startsWith("application.conf @ file");
    // should not read application-custom.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInCustomApplication")).isFalse();
  }

  @Test
  void should_create_application_config_with_custom_location() throws URISyntaxException {
    Path appConfigPath = Paths.get(getClass().getResource("/application-custom.conf").toURI());
    Config applicationConfig = ConfigUtils.createApplicationConfig(appConfigPath);
    assertReferenceConfig(applicationConfig);
    // should not read application.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInApplication")).isFalse();
    // should read application-custom.conf
    assertThat(applicationConfig.hasPath("dsbulk.definedInCustomApplication")).isTrue();
    assertThat(applicationConfig.getValue("dsbulk.definedInCustomApplication").origin().filename())
        .endsWith("application-custom.conf");
  }

  private static void assertReferenceConfig(Config referenceConfig) {
    // should read OSS driver reference.conf from its JAR
    assertThat(referenceConfig.hasPath("datastax-java-driver.basic.config-reload-interval"))
        .isTrue();
    assertThat(
            referenceConfig
                .getValue("datastax-java-driver.basic.config-reload-interval")
                .origin()
                .description())
        .startsWith("reference.conf @ jar");

    // should read DSE driver reference.conf from its JAR
    assertThat(referenceConfig.hasPath("datastax-java-driver.basic.load-balancing-policy.class"))
        .isTrue();
    assertThat(
            referenceConfig
                .getValue("datastax-java-driver.basic.load-balancing-policy.class")
                .origin()
                .description())
        .startsWith("reference.conf @ jar");

    // should read (dummy) dsbulk-reference.conf file found on the classpath
    assertThat(referenceConfig.hasPath("dsbulk.definedInDSBukReference")).isTrue();
    assertThat(referenceConfig.getValue("dsbulk.definedInDSBukReference").origin().description())
        .startsWith("dsbulk-reference.conf @ file");
  }

  @Test
  void should_get_absolute_path() {
    Config config = ConfigFactory.parseString("path = /var/lib");
    Path path = ConfigUtils.getPath(config, "path");
    assertThat(path).isNormalized().isAbsolute();
  }

  @Test
  void should_get_relative_path() {
    Config config = ConfigFactory.parseString("path1 = target, path2 = ./target");
    Path path1 = ConfigUtils.getPath(config, "path1");
    assertThat(path1).isNormalized().isAbsolute();
    Path path2 = ConfigUtils.getPath(config, "path2");
    assertThat(path2).isNormalized().isAbsolute();
    assertThat(path1).isEqualTo(path2);
  }

  @Test
  void should_get_path_with_user_home() {
    Config config = ConfigFactory.parseString("path1 = ~, path2 = ~/foo");
    Path home = Paths.get(System.getProperty("user.home"));
    Path path1 = ConfigUtils.getPath(config, "path1");
    assertThat(path1).isNormalized().isAbsolute().isEqualTo(home);
    Path path2 = ConfigUtils.getPath(config, "path2");
    assertThat(home.relativize(path2)).isEqualTo(Paths.get("foo"));
  }

  @Test
  void should_get_absolute_URL() throws Exception {
    Config config =
        ConfigFactory.parseString("url1 = \"file:///var/lib\", url2 = \"http://foo.com/bar\"");
    URL url1 = ConfigUtils.getURL(config, "url1");
    assertThat(url1.toExternalForm()).isEqualTo("file:/var/lib");
    assertThat(url1.toURI())
        .hasScheme("file")
        .hasNoPort()
        .hasNoQuery()
        .hasNoUserInfo()
        .hasPath("/var/lib");
    URL url2 = ConfigUtils.getURL(config, "url2");
    assertThat(url2.toExternalForm()).isEqualTo("http://foo.com/bar");
    assertThat(url2.toURI())
        .hasScheme("http")
        .hasNoPort()
        .hasNoQuery()
        .hasNoUserInfo()
        .hasAuthority("foo.com")
        .hasPath("/bar");
  }

  @Test
  void should_get_stdio_URL() throws Exception {
    Config config = ConfigFactory.parseString("url1 = -");
    URL stdioUrl = ConfigUtils.getURL(config, "url1");
    assertThat(stdioUrl.toExternalForm()).isEqualTo("std:/");
    assertThat(stdioUrl.toURI())
        .hasScheme("std")
        .hasNoPort()
        .hasNoQuery()
        .hasNoUserInfo()
        .hasPath("/");
  }

  @Test
  void should_get_threads() {
    Config config = ConfigFactory.parseString("threads1 = 4, threads2 = 2C");
    int threads1 = ConfigUtils.getThreads(config, "threads1");
    assertThat(threads1).isEqualTo(4);
    int threads2 = ConfigUtils.getThreads(config, "threads2");
    assertThat(threads2).isEqualTo(2 * Runtime.getRuntime().availableProcessors());
  }

  @Test
  void should_get_char() {
    Config config = ConfigFactory.parseString("char = a");
    char c = ConfigUtils.getChar(config, "char");
    assertThat(c).isEqualTo('a');
  }

  @Test
  void should_get_charset() {
    Config config = ConfigFactory.parseString("charset1 = UTF-8, charset2 = utf8");
    Charset charset1 = ConfigUtils.getCharset(config, "charset1");
    assertThat(charset1).isEqualTo(StandardCharsets.UTF_8);
    Charset charset2 = ConfigUtils.getCharset(config, "charset2");
    assertThat(charset2).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void should_resolve_path() {
    // relative paths, should behave the same on all platforms
    assertThat(ConfigUtils.resolvePath("~")).isEqualTo(Paths.get(System.getProperty("user.home")));
    assertThat(ConfigUtils.resolvePath("~/foo"))
        .isEqualTo(Paths.get(System.getProperty("user.home"), "foo"));
    assertThat(ConfigUtils.resolvePath("foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar"));
    assertThat(ConfigUtils.resolvePath("./foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar"));
    assertThat(ConfigUtils.resolvePath("./foo/../foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar"));
    assertThatThrownBy(() -> ConfigUtils.resolvePath("~otheruser/foo"))
        .isInstanceOf(InvalidPathException.class)
        .hasMessageContaining("Cannot resolve home directory");
    // absolute and invalid paths must be tested on a per-platform basis
    assumingThat(
        !isWindows(),
        () -> {
          assertThat(ConfigUtils.resolvePath("/foo/bar")).isEqualTo(Paths.get("/foo/bar"));
          assertThatThrownBy(() -> ConfigUtils.resolvePath("\u0000"))
              .isInstanceOf(InvalidPathException.class)
              .hasMessageContaining("Nul character not allowed");
        });
    assumingThat(
        isWindows(),
        () -> {
          assertThat(ConfigUtils.resolvePath("C:\\foo\\bar")).isEqualTo(Paths.get("C:\\foo\\bar"));
          assertThatThrownBy(() -> ConfigUtils.resolvePath("C:\\should:\\fail"))
              .isInstanceOf(InvalidPathException.class)
              .hasMessageContaining("Illegal char <:> at index");
        });
  }

  @Test
  void should_resolve_url() throws MalformedURLException {
    assertThat(ConfigUtils.resolveURL("-")).isEqualTo(new URL("std:/"));
    assertThat(ConfigUtils.resolveURL("http://acme.com")).isEqualTo(new URL("http://acme.com"));
    assumingThat(
        !isWindows(),
        () ->
            assertThatThrownBy(
                    () -> ConfigUtils.resolveURL("nonexistentscheme://should/fail/\u0000"))
                .isInstanceOf(InvalidPathException.class)
                .satisfies(
                    t -> assertThat(t.getSuppressed()[0]).isInstanceOf(MalformedURLException.class))
                .hasMessageContaining("Nul character not allowed"));
    assumingThat(
        isWindows(),
        () ->
            assertThatThrownBy(() -> ConfigUtils.resolveURL("nonexistentscheme://should/fail"))
                .isInstanceOf(InvalidPathException.class)
                .satisfies(
                    t -> assertThat(t.getSuppressed()[0]).isInstanceOf(MalformedURLException.class))
                .hasMessageContaining("Illegal char <:> at index 17"));
    assertThat(ConfigUtils.resolveURL("~"))
        .isEqualTo(Paths.get(System.getProperty("user.home")).toUri().normalize().toURL());
    assertThat(ConfigUtils.resolveURL("~/foo"))
        .isEqualTo(Paths.get(System.getProperty("user.home"), "foo").toUri().toURL());
    assumingThat(
        !isWindows(),
        () ->
            assertThat(ConfigUtils.resolveURL("/foo/bar"))
                .isEqualTo(Paths.get("/foo/bar").toUri().toURL()));
    assumingThat(
        isWindows(),
        () ->
            assertThat(ConfigUtils.resolveURL("C:/foo/bar"))
                .isEqualTo(Paths.get("C:/foo/bar").toUri().toURL()));
    assertThat(ConfigUtils.resolveURL("foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar").toUri().toURL());
    assertThat(ConfigUtils.resolveURL("./foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar").toUri().toURL());
    assertThat(ConfigUtils.resolveURL("./foo/../foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar").toUri().toURL());
    assertThatThrownBy(() -> ConfigUtils.resolveURL("~otheruser/foo"))
        .isInstanceOf(InvalidPathException.class)
        .hasMessageContaining("Cannot resolve home directory");
  }

  @Test
  void should_resolve_user_home() {
    assertThat(ConfigUtils.resolveUserHome("~"))
        .contains(Paths.get(System.getProperty("user.home")));
    assertThat(ConfigUtils.resolveUserHome("~/foo"))
        .contains(Paths.get(System.getProperty("user.home"), "foo"));
    assertThatThrownBy(() -> ConfigUtils.resolveUserHome("~otheruser/foo"))
        .isInstanceOf(InvalidPathException.class)
        .hasMessageContaining("Cannot resolve home directory");
  }

  @Test
  void should_resolve_threads() {
    assertThat(ConfigUtils.resolveThreads("123")).isEqualTo(123);
    assertThat(ConfigUtils.resolveThreads(" 8 c"))
        .isEqualTo(8 * Runtime.getRuntime().availableProcessors());
    assertThatThrownBy(() -> ConfigUtils.resolveThreads("should fail"))
        .isInstanceOf(PatternSyntaxException.class)
        .hasMessageContaining("Cannot parse input as N * <num_cores>");
  }

  @Test
  void should_get_type_string() {
    Config config =
        ConfigFactory.parseString(
            "intField = 7, "
                + "stringField = mystring, "
                + "stringListField = [\"v1\", \"v2\"], "
                + "numberListField = [9, 7], "
                + "booleanField = false");
    assertThat(ConfigUtils.getTypeString(config, "intField")).contains("number");
    assertThat(ConfigUtils.getTypeString(config, "stringField")).contains("string");
    assertThat(ConfigUtils.getTypeString(config, "stringListField")).contains("list<string>");
    assertThat(ConfigUtils.getTypeString(config, "numberListField")).contains("list<number>");
    assertThat(ConfigUtils.getTypeString(config, "booleanField")).contains("boolean");
    assertThatThrownBy(() -> ConfigUtils.getTypeString(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_null_safe_value() {
    Config config =
        ConfigFactory.parseString(
            "intField = 7, "
                + "stringField = mystring, "
                + "stringListField = [\"v1\", \"v2\"], "
                + "numberListField = [9, 7], "
                + "nullField = [9, 7], "
                + "booleanField = false,"
                + "nullField = null");
    assertThat(ConfigUtils.getNullSafeValue(config, "intField").valueType()).isEqualTo(NUMBER);
    assertThat(ConfigUtils.getNullSafeValue(config, "stringField").valueType()).isEqualTo(STRING);
    assertThat(ConfigUtils.getNullSafeValue(config, "stringListField").valueType()).isEqualTo(LIST);
    assertThat(ConfigUtils.getNullSafeValue(config, "numberListField").valueType()).isEqualTo(LIST);
    assertThat(ConfigUtils.getNullSafeValue(config, "booleanField").valueType()).isEqualTo(BOOLEAN);
    assertThat(ConfigUtils.getNullSafeValue(config, "nullField").valueType()).isEqualTo(NULL);
    assertThatThrownBy(() -> ConfigUtils.getNullSafeValue(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_config_value_type() {
    Config config =
        ConfigFactory.parseString(
            "intField = 7\n"
                + "stringField = mystring\n"
                + "stringListField = [\"v1\", \"v2\"]\n"
                + "numberListField = [9, 7]\n"
                + "nullField = [9, 7]\n"
                + "booleanField = false\n"
                + "# @type string\n"
                + "nullField = null");
    assertThat(ConfigUtils.getValueType(config, "intField")).isEqualTo(NUMBER);
    assertThat(ConfigUtils.getValueType(config, "stringField")).isEqualTo(STRING);
    assertThat(ConfigUtils.getValueType(config, "stringListField")).isEqualTo(LIST);
    assertThat(ConfigUtils.getValueType(config, "numberListField")).isEqualTo(LIST);
    assertThat(ConfigUtils.getValueType(config, "booleanField")).isEqualTo(BOOLEAN);
    assertThat(ConfigUtils.getValueType(config, "nullField")).isEqualTo(STRING);
    assertThatThrownBy(() -> ConfigUtils.getValueType(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_type_hint_for_value() {
    Config config =
        ConfigFactory.parseString("# This is a comment.\n# @type string\nmyField = foo");
    assertThat(ConfigUtils.getTypeHint(config.getValue("myField"))).contains("string");
  }

  @Test
  void should_get_comments_for_value() {
    Config config =
        ConfigFactory.parseString("# This is a comment.\n# @type string\nmyField = foo");
    assertThat(ConfigUtils.getComments(config.getValue("myField"))).isEqualTo("This is a comment.");
  }

  @ParameterizedTest
  @MethodSource("urlsProvider")
  void should_get_urls_from_file(List<String> input, List<URL> expected) throws IOException {
    // given
    Path urlFile = createURLFile(input);

    // when
    List<URL> urlsFromFile = ConfigUtils.getURLsFromFile(urlFile);

    // then
    assertThat(urlsFromFile).isEqualTo(expected);
    Files.delete(urlFile);
  }

  @Test
  void should_throw_if_provided_urlfile_path_not_exists() {
    // given
    Path urlFile = Paths.get("/non-existing");

    // when
    assertThatThrownBy(() -> ConfigUtils.getURLsFromFile(urlFile))
        .isInstanceOf(NoSuchFileException.class);
  }

  @Test
  void should_handle_file_with_non_ascii_url() throws IOException {
    // given
    Path urlFile = createURLFile(new URL("http://foo.com/bar?param=καλημέρα"));

    // when
    List<URL> urlsFromFile = ConfigUtils.getURLsFromFile(urlFile);

    // then
    assertThat(urlsFromFile).containsExactly(new URL("http://foo.com/bar?param=καλημέρα"));

    Files.delete(urlFile);
  }

  @Test
  void should_detect_value_from_reference() {
    Config config =
        ConfigFactory.parseString(
                "overriddenInApplication = -1, onlyInApplication = -2, onlyInApplicationButNull = null")
            .withFallback(ConfigFactory.defaultReference());
    assertThat(ConfigUtils.hasReferenceValue(config, "overriddenInApplication")).isFalse();
    assertThat(ConfigUtils.hasReferenceValue(config, "onlyInApplication")).isFalse();
    assertThat(ConfigUtils.hasReferenceValue(config, "onlyInApplicationButNull")).isFalse();
    assertThat(ConfigUtils.hasReferenceValue(config, "fromReference")).isTrue();
    assertThat(ConfigUtils.hasReferenceValue(config, "fromReferenceButNull")).isTrue();
    assertThat(ConfigUtils.hasReferenceValue(config, "nonExistent")).isFalse();
  }

  @Test
  void should_detect_user_defined_value() {
    Config config =
        ConfigFactory.parseString(
                "overriddenInApplication = -1, onlyInApplication = -2, onlyInApplicationButNull = null")
            .withFallback(ConfigFactory.defaultReference());
    assertThat(ConfigUtils.hasUserOverride(config, "overriddenInApplication")).isTrue();
    assertThat(ConfigUtils.hasUserOverride(config, "onlyInApplication")).isTrue();
    assertThat(ConfigUtils.hasUserOverride(config, "onlyInApplicationButNull")).isTrue();
    assertThat(ConfigUtils.hasUserOverride(config, "fromReference")).isFalse();
    assertThat(ConfigUtils.hasUserOverride(config, "fromReferenceButNull")).isFalse();
    assertThat(ConfigUtils.hasUserOverride(config, "nonExistent")).isFalse();
  }

  static List<Arguments> urlsProvider() throws MalformedURLException {
    if (isWindows()) {
      return Lists.newArrayList(
          arguments(
              Arrays.asList("C:\\a\\first-file", "D:/a/second-file"),
              Arrays.asList(new URL("file:/C:/a/first-file"), new URL("file:/D:/a/second-file"))),
          arguments(
              Arrays.asList("C:\\a\\first-file", "# this is a comment line"),
              Collections.singletonList(new URL("file:/C:/a/first-file"))),
          arguments(
              Arrays.asList("C:\\a\\first-file", " D:/a/second-file "),
              Arrays.asList(new URL("file:/C:/a/first-file"), new URL("file:/D:/a/second-file"))));
    } else {
      return Lists.newArrayList(
          arguments(
              Arrays.asList("/a-first-file", "/second-file"),
              Arrays.asList(new URL("file:/a-first-file"), new URL("file:/second-file"))),
          arguments(
              Arrays.asList("/a-first-file", "# this is a comment line"),
              Collections.singletonList(new URL("file:/a-first-file"))),
          arguments(
              Arrays.asList("/a-first-file", " /second-file "),
              Arrays.asList(new URL("file:/a-first-file"), new URL("file:/second-file"))));
    }
  }

  @ParameterizedTest
  @MethodSource
  void should_create_exception_with_expected_message(
      ConfigException ce, String basePath, String expectedErrorMessage) {
    IllegalArgumentException bce = ConfigUtils.convertConfigException(ce, basePath);
    assertThat(bce).hasMessageContaining(expectedErrorMessage);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_create_exception_with_expected_message() {
    return Stream.of(
        // ConfigException.WrongType
        Arguments.of(
            catchThrowable(() -> ConfigFactory.parseString("key=NotANumber").getInt("key")),
            "base.path",
            "Invalid value for base.path.key, expecting NUMBER, got STRING [at: String: 1]"),
        // ConfigException.BadValue (enum)
        Arguments.of(
            catchThrowable(
                () -> ConfigFactory.parseString("key=NotAnEnum").getEnum(MyENum.class, "key")),
            "base.path",
            "Invalid value for base.path.key, expecting one of FOO, BAR, got: 'NotAnEnum' [at: String: 1]"),
        // ConfigException.BadValue (not enum)
        Arguments.of(
            catchThrowable(() -> ConfigFactory.parseString("key=NotADuration").getDuration("key")),
            "base.path",
            "Invalid value for base.path.key: No number in duration value 'NotADuration' [at: String: 1]"),
        // ConfigException.Null (enum)
        Arguments.of(
            catchThrowable(() -> ConfigFactory.parseString("key=null").getString("key")),
            "base.path",
            "Invalid value for base.path.key, expecting STRING, got NULL [at: String: 1]"),
        // ConfigException.Parse
        Arguments.of(
            catchThrowable(() -> ConfigFactory.parseString("key=[ParseError").getStringList("key")),
            "base.path",
            "List should have ended with ] or had a comma"),
        // base path empty
        Arguments.of(
            catchThrowable(() -> ConfigFactory.parseString("key=NotANumber").getInt("key")),
            "",
            "Invalid value for key, expecting NUMBER, got STRING [at: String: 1]"),
        // ConfigException.Other
        Arguments.of(new BadBean("Not a standard message"), "base.path", "Not a standard message"));
  }

  @Test
  void should_read_password() {
    // given
    Console console = mock(Console.class);
    String path = "my.password";
    String password = "fakePasswordForTests";
    when(console.readPassword("Please input value for setting %s: ", path))
        .thenReturn(password.toCharArray());
    Config config = ConfigFactory.empty();
    // when
    config = ConfigUtils.readPassword(config, path, console);
    // then
    assertThat(config.hasPath(path)).isTrue();
    assertThat(config.getString(path)).isEqualTo(password);
    assertThat(config.getValue(path).origin().description()).isEqualTo("stdin");
    verify(console).readPassword("Please input value for setting %s: ", path);
    verifyNoMoreInteractions(console);
  }

  private static Path createURLFile(URL... urls) throws IOException {
    Path file = Files.createTempFile("urlfile", null);
    Files.write(
        file,
        Arrays.stream(urls).map(URL::toExternalForm).collect(Collectors.toList()),
        StandardCharsets.UTF_8);
    return file;
  }

  private static Path createURLFile(List<String> urls) throws IOException {
    Path file = Files.createTempFile("urlfile", null);
    Files.write(file, urls, StandardCharsets.UTF_8);
    return file;
  }

  @SuppressWarnings("unused")
  private enum MyENum {
    FOO,
    BAR
  }

  private static boolean isWindows() {
    String osName = System.getProperty("os.name");
    return osName != null && osName.startsWith("Windows");
  }
}
