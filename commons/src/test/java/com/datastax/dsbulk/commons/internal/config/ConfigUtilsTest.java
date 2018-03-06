/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolvePath;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolveThreads;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.resolveURL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.utils.URLUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

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
}
