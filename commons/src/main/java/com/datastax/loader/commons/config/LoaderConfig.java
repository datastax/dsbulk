/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.commons.config;

import com.datastax.loader.commons.internal.reflection.ReflectionUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface LoaderConfig extends Config {

  /**
   * Returns a new instance of the class name at the given path.
   *
   * <p>Short class names are allowed and will be resolved against common package names.
   *
   * @param <T> the expected type.
   * @param path path expression.
   * @return the newly-allocated object corresponding to the class name at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   */
  default <T> T getInstance(String path) {
    String setting = getString(path);
    try {
      return ReflectionUtils.newInstance(setting);
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(), path, "FQCN or short class name", getValue(path).valueType().toString(), e);
    }
  }

  /**
   * Returns the {@link Class} object at the given path.
   *
   * <p>Short class names are allowed and will be resolved against common package names.
   *
   * @param <T> the expected type.
   * @param path path expression.
   * @return the Class object corresponding to the class name at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   */
  default <T> Class<T> getClass(String path) {
    String setting = getString(path);
    try {
      return ReflectionUtils.resolveClass(setting);
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(), path, "FQCN or short class name", getValue(path).valueType().toString(), e);
    }
  }

  /**
   * Returns the {@link Path} object at the given path.
   *
   * <p>The returned Path is normalized and absolute.
   *
   * @param path path expression.
   * @return the Path object at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   */
  default Path getPath(String path) {
    return Paths.get(getString(path)).normalize().toAbsolutePath();
  }

  /**
   * Returns the {@link URL} object at the given path.
   *
   * <p>The value will be first interpreted directly as a URL; if the parsing fails, the value will
   * be then interpreted as a path on the local filesystem, then converted to a file URL.
   *
   * <p>The returned URL is normalized and absolute.
   *
   * @param path path expression.
   * @return the URL object at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a URL.
   */
  default URL getURL(String path) {
    String setting = getString(path);
    try {
      return new URI(setting).normalize().toURL();
    } catch (Exception e) {
      // not a valid URL, consider it a path on the local filesystem.
      try {
        return getPath(path).toUri().toURL();
      } catch (Exception e1) {
        e1.addSuppressed(e);
        throw new ConfigException.WrongType(
            origin(), path, "path or URL", getValue(path).valueType().toString(), e1);
      }
    }
  }

  /**
   * Returns the number of threads at the given path.
   *
   * <p>The given path can be an integer, or alternatively, an integer followed by the letter C, in
   * which case, the resulting value is the integer multiplied by the number of available cores on
   * the system.
   *
   * @param path path expression.
   * @return the number of threads at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a number of threads.
   */
  default int getThreads(String path) {
    String setting = getString(path);
    int threads;
    try {
      threads = Integer.parseInt(setting);
    } catch (NumberFormatException e) {
      Pattern pattern = Pattern.compile("(\\d+)\\s*C", Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(setting);
      if (matcher.find()) {
        threads = Runtime.getRuntime().availableProcessors() * Integer.parseInt(matcher.group(1));
      } else {
        throw new ConfigException.WrongType(
            origin(),
            path,
            "integer or string in 'nC' syntax",
            getValue(path).valueType().toString());
      }
    }
    return threads;
  }

  /**
   * Returns the character at the given path.
   *
   * @param path path expression.
   * @return the character at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a single character.
   */
  default char getChar(String path) {
    String setting = getString(path);
    if (setting.length() != 1) {
      throw new ConfigException.WrongType(
          origin(), path, "single character", getValue(path).valueType().toString());
    }
    return setting.charAt(0);
  }

  /**
   * Returns the {@link Charset} at the given path.
   *
   * @param path path expression.
   * @return the Charset at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Charset.
   */
  default Charset getCharset(String path) {
    String setting = getString(path);
    try {
      return Charset.forName(setting);
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(), path, "valid charset name", getValue(path).valueType().toString(), e);
    }
  }

  /**
   * Returns the first string in the string list at the given path, if it exists, or null otherwise.
   *
   * @param path path expression.
   * @return the the first string in the string list at the given path, if it exists, or null
   *     otherwise.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a String list.
   */
  default String getFirstString(String path) {
    List<String> list = getStringList(path);
    if (list.isEmpty()) {
      return null;
    }
    return list.get(0);
  }

  @Override
  LoaderConfig withFallback(ConfigMergeable other);

  @Override
  LoaderConfig resolve(ConfigResolveOptions options);

  @Override
  LoaderConfig resolveWith(Config source);

  @Override
  LoaderConfig resolveWith(Config source, ConfigResolveOptions options);

  @Override
  LoaderConfig getConfig(String path);

  @Override
  LoaderConfig withOnlyPath(String path);

  @Override
  LoaderConfig withoutPath(String path);

  @Override
  LoaderConfig atPath(String path);

  @Override
  LoaderConfig atKey(String key);

  @Override
  LoaderConfig withValue(String path, ConfigValue value);
}
