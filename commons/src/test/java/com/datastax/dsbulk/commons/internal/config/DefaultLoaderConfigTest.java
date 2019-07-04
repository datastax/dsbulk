/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.utils.URLUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultLoaderConfigTest {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  @Test
  void should_resolve_absolute_path() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.parseString("path = /var/lib"));
    Path path = config.getPath("path");
    assertThat(path).isNormalized().isAbsolute();
  }

  @Test
  void should_resolve_relative_path() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("path1 = target, path2 = ./target"));
    Path path1 = config.getPath("path1");
    assertThat(path1).isNormalized().isAbsolute();
    Path path2 = config.getPath("path2");
    assertThat(path2).isNormalized().isAbsolute();
    assertThat(path1).isEqualTo(path2);
  }

  @Test
  void should_resolve_user_home() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("path1 = ~, path2 = ~/foo"));
    Path home = Paths.get(System.getProperty("user.home"));
    Path path1 = config.getPath("path1");
    assertThat(path1).isNormalized().isAbsolute().isEqualTo(home);
    Path path2 = config.getPath("path2");
    assertThat(home.relativize(path2)).isEqualTo(Paths.get("foo"));
  }

  @Test
  void should_resolve_absolute_URL() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("url1 = \"file:///var/lib\", url2 = \"http://foo.com/bar\""));
    URL url1 = config.getURL("url1");
    assertThat(url1.toExternalForm()).isEqualTo("file:/var/lib");
    assertThat(url1.toURI())
        .hasScheme("file")
        .hasNoPort()
        .hasNoQuery()
        .hasNoUserInfo()
        .hasPath("/var/lib");
    URL url2 = config.getURL("url2");
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
  void should_resolve_stdio_URL() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.parseString("url1 = -"));

    URL stdioUrl = config.getURL("url1");
    assertThat(stdioUrl.toExternalForm()).isEqualTo("std:/");
    assertThat(stdioUrl.toURI())
        .hasScheme("std")
        .hasNoPort()
        .hasNoQuery()
        .hasNoUserInfo()
        .hasPath("/");
  }

  @Test
  void should_resolve_threads() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("threads1 = 4, threads2 = 2C"));
    int threads1 = config.getThreads("threads1");
    assertThat(threads1).isEqualTo(4);
    int threads2 = config.getThreads("threads2");
    assertThat(threads2).isEqualTo(2 * Runtime.getRuntime().availableProcessors());
  }

  @Test
  void should_resolve_char() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.parseString("char = a"));
    char c = config.getChar("char");
    assertThat(c).isEqualTo('a');
  }

  @Test
  void should_resolve_charset() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("charset1 = UTF-8, charset2 = utf8"));
    Charset charset1 = config.getCharset("charset1");
    assertThat(charset1).isEqualTo(Charset.forName("UTF-8"));
    Charset charset2 = config.getCharset("charset2");
    assertThat(charset2).isEqualTo(Charset.forName("UTF-8"));
  }

  @Test
  void should_resolve_class() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("class1 = java.lang.String, class2 = DefaultRetryPolicy"));
    Class<? extends String> class1 = config.getClass("class1", String.class);
    assertThat(class1).isEqualTo(String.class);
    Class<?> class2 = config.getClass("class2", RetryPolicy.class);
    assertThat(class2).isEqualTo(DefaultRetryPolicy.class);
  }

  @Test
  void should_resolve_instance() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "class1 = java.lang.String, class2 = AtomicMonotonicTimestampGenerator"));
    Object o1 = config.getInstance("class1", String.class);
    assertThat(o1).isInstanceOf(String.class);
    Object o2 = config.getInstance("class2", TimestampGenerator.class);
    assertThat(o2).isInstanceOf(AtomicMonotonicTimestampGenerator.class);
  }

  @ParameterizedTest
  @MethodSource("urlsProvider")
  void should_get_urls_from_file(
      List<String> input, List<URL> expectedNonWindows, List<URL> expectedWindows)
      throws IOException {
    // given
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.parseString(""));
    String urlFile = createUrlFile(input);

    // when
    List<URL> urlsFromFile = config.getUrlsFromFile(urlFile, Charset.defaultCharset());

    // then
    assumingThat(
        !PlatformUtils.isWindows(), () -> assertThat(urlsFromFile).isEqualTo(expectedNonWindows));
    assumingThat(
        PlatformUtils.isWindows(), () -> assertThat(urlsFromFile).isEqualTo(expectedWindows));
    Files.delete(Paths.get(urlFile));
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

  private static String createUrlFile(List<String> urls) throws IOException {
    File file = File.createTempFile("urlfile", null);
    Files.write(file.toPath(), urls, Charset.defaultCharset());
    return file.getAbsolutePath();
  }
}
