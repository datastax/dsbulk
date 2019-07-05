/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getComments;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getNullSafeValue;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getTypeHint;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getTypeString;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getValueType;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolvePath;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolveThreads;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolveURL;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.createURLFile;
import static com.typesafe.config.ConfigValueType.BOOLEAN;
import static com.typesafe.config.ConfigValueType.LIST;
import static com.typesafe.config.ConfigValueType.NULL;
import static com.typesafe.config.ConfigValueType.NUMBER;
import static com.typesafe.config.ConfigValueType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.utils.URLUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigUtilsTest {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  @Test
  void should_resolve_path() {
    // relative paths, should behave the same on all platforms
    assertThat(resolvePath("~")).isEqualTo(Paths.get(System.getProperty("user.home")));
    assertThat(resolvePath("~/foo")).isEqualTo(Paths.get(System.getProperty("user.home"), "foo"));
    assertThat(resolvePath("foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar"));
    assertThatThrownBy(() -> resolvePath("~otheruser/foo"))
        .isInstanceOf(InvalidPathException.class)
        .hasMessageContaining("Cannot resolve home directory");
    // absolute and invalid paths must be tested on a per-platform basis
    assumingThat(
        !PlatformUtils.isWindows(),
        () -> {
          assertThat(resolvePath("/foo/bar")).isEqualTo(Paths.get("/foo/bar"));
          assertThatThrownBy(() -> resolvePath("\u0000"))
              .isInstanceOf(InvalidPathException.class)
              .hasMessageContaining("Nul character not allowed");
        });
    assumingThat(
        PlatformUtils.isWindows(),
        () -> {
          assertThat(resolvePath("C:\\foo\\bar")).isEqualTo(Paths.get("C:\\foo\\bar"));
          assertThatThrownBy(() -> resolvePath("C:\\should:\\fail"))
              .isInstanceOf(InvalidPathException.class)
              .hasMessageContaining("Illegal char <:> at index");
        });
  }

  @Test
  void should_resolve_url() throws MalformedURLException {
    assertThat(resolveURL("-")).isEqualTo(new URL("std:/"));
    assertThat(resolveURL("http://acme.com")).isEqualTo(new URL("http://acme.com"));
    assumingThat(
        !PlatformUtils.isWindows(),
        () ->
            assertThatThrownBy(() -> resolveURL("nonexistentscheme://should/fail/\u0000"))
                .isInstanceOf(InvalidPathException.class)
                .satisfies(
                    t -> assertThat(t.getSuppressed()[0]).isInstanceOf(MalformedURLException.class))
                .hasMessageContaining("Nul character not allowed"));
    assumingThat(
        PlatformUtils.isWindows(),
        () ->
            assertThatThrownBy(() -> resolveURL("nonexistentscheme://should/fail"))
                .isInstanceOf(InvalidPathException.class)
                .satisfies(
                    t -> assertThat(t.getSuppressed()[0]).isInstanceOf(MalformedURLException.class))
                .hasMessageContaining("Illegal char <:> at index 17"));
    assertThat(resolveURL("~"))
        .isEqualTo(Paths.get(System.getProperty("user.home")).toUri().toURL());
    assertThat(resolveURL("~/foo"))
        .isEqualTo(Paths.get(System.getProperty("user.home"), "foo").toUri().toURL());
    assertThat(resolveURL("/foo/bar")).isEqualTo(Paths.get("/foo/bar").toUri().toURL());
    assertThat(resolveURL("foo/bar"))
        .isEqualTo(Paths.get(System.getProperty("user.dir"), "foo", "bar").toUri().toURL());
    assertThatThrownBy(() -> resolveURL("~otheruser/foo"))
        .isInstanceOf(InvalidPathException.class)
        .satisfies(t -> assertThat(t.getSuppressed()[0]).isInstanceOf(MalformedURLException.class))
        .hasMessageContaining("Cannot resolve home directory");
  }

  @Test
  void should_resolve_threads() {
    assertThat(resolveThreads("123")).isEqualTo(123);
    assertThat(resolveThreads(" 8 c")).isEqualTo(8 * Runtime.getRuntime().availableProcessors());
    assertThatThrownBy(() -> resolveThreads("should fail"))
        .isInstanceOf(PatternSyntaxException.class)
        .hasMessageContaining("Cannot parse input as N * <num_cores>");
  }

  @Test
  void should_get_type_string() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "intField = 7, "
                    + "stringField = mystring, "
                    + "stringListField = [\"v1\", \"v2\"], "
                    + "numberListField = [9, 7], "
                    + "booleanField = false"));
    assertThat(getTypeString(config, "intField")).isEqualTo("number");
    assertThat(getTypeString(config, "stringField")).isEqualTo("string");
    assertThat(getTypeString(config, "stringListField")).isEqualTo("list<string>");
    assertThat(getTypeString(config, "numberListField")).isEqualTo("list<number>");
    assertThat(getTypeString(config, "booleanField")).isEqualTo("boolean");
    assertThatThrownBy(() -> getTypeString(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_null_safe_value() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "intField = 7, "
                    + "stringField = mystring, "
                    + "stringListField = [\"v1\", \"v2\"], "
                    + "numberListField = [9, 7], "
                    + "nullField = [9, 7], "
                    + "booleanField = false,"
                    + "nullField = null"));
    assertThat(getNullSafeValue(config, "intField").valueType()).isEqualTo(NUMBER);
    assertThat(getNullSafeValue(config, "stringField").valueType()).isEqualTo(STRING);
    assertThat(getNullSafeValue(config, "stringListField").valueType()).isEqualTo(LIST);
    assertThat(getNullSafeValue(config, "numberListField").valueType()).isEqualTo(LIST);
    assertThat(getNullSafeValue(config, "booleanField").valueType()).isEqualTo(BOOLEAN);
    assertThat(getNullSafeValue(config, "nullField").valueType()).isEqualTo(NULL);
    assertThatThrownBy(() -> getNullSafeValue(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_config_value_type() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "intField = 7\n"
                    + "stringField = mystring\n"
                    + "stringListField = [\"v1\", \"v2\"]\n"
                    + "numberListField = [9, 7]\n"
                    + "nullField = [9, 7]\n"
                    + "booleanField = false\n"
                    + "# @type string\n"
                    + "nullField = null"));
    assertThat(getValueType(config, "intField")).isEqualTo(NUMBER);
    assertThat(getValueType(config, "stringField")).isEqualTo(STRING);
    assertThat(getValueType(config, "stringListField")).isEqualTo(LIST);
    assertThat(getValueType(config, "numberListField")).isEqualTo(LIST);
    assertThat(getValueType(config, "booleanField")).isEqualTo(BOOLEAN);
    assertThat(getValueType(config, "nullField")).isEqualTo(STRING);
    assertThatThrownBy(() -> getValueType(config, "this.path.does.not.exist"))
        .isInstanceOf(ConfigException.Missing.class)
        .hasMessage("No configuration setting found for key 'this.path.does.not.exist'");
  }

  @Test
  void should_get_type_hint_for_value() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("# This is a comment.\n# @type string\nmyField = foo"));
    assertThat(getTypeHint(config.getValue("myField"))).contains("string");
  }

  @Test
  void should_get_comments_for_value() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("# This is a comment.\n# @type string\nmyField = foo"));
    assertThat(getComments(config.getValue("myField"))).isEqualTo("This is a comment.");
  }

  @ParameterizedTest
  @MethodSource("urlsProvider")
  void should_get_urls_from_file(
      List<String> input, List<URL> expectedNonWindows, List<URL> expectedWindows)
      throws IOException {
    // given
    Path urlFile = createURLFile(input);

    // when
    List<URL> urlsFromFile = ConfigUtils.getURLsFromFile(urlFile);

    // then
    assumingThat(
        !PlatformUtils.isWindows(), () -> assertThat(urlsFromFile).isEqualTo(expectedNonWindows));
    assumingThat(
        PlatformUtils.isWindows(), () -> assertThat(urlsFromFile).isEqualTo(expectedWindows));
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
  void should_handle_file_with_non_ascii_url_decoded() throws IOException {
    // given
    Path urlFile =
        createURLFile(
            new URL("http://foo.com/bar?param=%CE%BA%CE%B1%CE%BB%CE%B7%CE%BC%CE%AD%CF%81%CE%B1"));

    // when
    List<URL> urlsFromFile = ConfigUtils.getURLsFromFile(urlFile);

    // then
    assertThat(urlsFromFile).containsExactly(new URL("http://foo.com/bar?param=καλημέρα"));

    Files.delete(urlFile);
  }

  static List<Arguments> urlsProvider() throws MalformedURLException {
    return Lists.newArrayList(
        arguments(
            Arrays.asList("/a-first-file", "/second-file"),
            Arrays.asList(new URL("file:/a-first-file"), new URL("file:/second-file")),
            Arrays.asList(new URL("file:/C:/a-first-file"), new URL("file:/C:/second-file"))),
        arguments(
            Arrays.asList("/a-first-file", "#/second-file"),
            Collections.singletonList(new URL("file:/a-first-file")),
            Collections.singletonList(new URL("file:/C:/a-first-file"))),
        arguments(
            Arrays.asList("/a-first-file", "/second-file "),
            Arrays.asList(new URL("file:/a-first-file"), new URL("file:/second-file")),
            Arrays.asList(new URL("file:/C:/a-first-file"), new URL("file:/C:/second-file"))));
  }
}
