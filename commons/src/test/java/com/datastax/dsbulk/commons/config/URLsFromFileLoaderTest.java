/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class URLsFromFileLoaderTest {
  @ParameterizedTest
  @MethodSource("urlsProvider")
  void should_get_urls_from_file(
      List<String> input, List<URL> expectedNonWindows, List<URL> expectedWindows)
      throws IOException {
    // given
    Path urlFile = createURLFile(input);

    // when
    List<URL> urlsFromFile = URLsFromFileLoader.getURLs(urlFile, Charset.forName("UTF-8"));

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
    assertThatThrownBy(() -> URLsFromFileLoader.getURLs(urlFile, Charset.forName("UTF-8")))
        .isInstanceOf(NoSuchFileException.class);
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

  private static Path createURLFile(List<String> urls) throws IOException {
    File file = File.createTempFile("urlfile", null);
    Files.write(file.toPath(), urls, Charset.forName("UTF-8"));
    return file.toPath();
  }
}
