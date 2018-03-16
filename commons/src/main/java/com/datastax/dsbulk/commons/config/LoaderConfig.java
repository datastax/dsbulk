/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.config;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.reflection.ReflectionUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public interface LoaderConfig extends Config {

  String TYPE_ANNOTATION = "@type";

  String LEAF_ANNOTATION = "@leaf";

  /**
   * Returns a new instance of the class name at the given path.
   *
   * <p>Short class names are allowed and will be resolved against common package names.
   *
   * @param <T> the expected type.
   * @param path path expression.
   * @param expected The expected class or interface that the object should be an instance of.
   * @return the newly-allocated object corresponding to the class name at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   * @throws ConfigException.BadValue if the object is not of the expected type.
   */
  default <T> T getInstance(String path, Class<T> expected) {
    String setting = getString(path);
    try {
      Object o = ReflectionUtils.newInstance(setting);
      if (expected.isAssignableFrom(o.getClass())) {
        @SuppressWarnings("unchecked")
        T ret = (T) o;
        return ret;
      }
      throw new ConfigException.BadValue(
          origin(),
          path,
          String.format(
              "Object does not extend nor implement %s: %s",
              expected.getSimpleName(), o.getClass().getSimpleName()));
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(),
          String.format("%s: Expecting FQCN or short class name, got '%s'", path, setting),
          e);
    }
  }

  /**
   * Returns the {@link Class} object at the given path.
   *
   * <p>Short class names are allowed and will be resolved against common package names.
   *
   * @param <T> the expected type.
   * @param path path expression.
   * @param expected The expected class or interface that the object should be an instance of.
   * @return the Class object corresponding to the class name at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   * @throws ConfigException.BadValue if the object is not of the expected type.
   */
  default <T> Class<? extends T> getClass(String path, Class<T> expected) {
    String setting = getString(path);
    try {
      Class<?> c = ReflectionUtils.resolveClass(setting);
      if (expected.isAssignableFrom(c)) {
        @SuppressWarnings("unchecked")
        Class<T> ret = (Class<T>) c;
        return ret;
      }
      throw new ConfigException.BadValue(
          origin(),
          path,
          String.format(
              "Class does not extend nor implement %s: %s",
              expected.getSimpleName(), c.getSimpleName()));
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(),
          String.format("%s: Expecting FQCN or short class name, got '%s'", path, setting),
          e);
    }
  }

  /**
   * Returns the {@link Path} object at the given path.
   *
   * <p>The returned Path is normalized and absolute.
   *
   * <p>For convenience, if the path begins with a tilde (`~`), that symbol will be expanded to the
   * current user's home directory, as supplied by `System.getProperty("user.home")`. Note that this
   * expansion will not occur when the tilde is not the first character in the path, nor when the
   * home directory owner is not the current user.
   *
   * @param path path expression.
   * @return the Path object at the requested path.
   * @throws ConfigException.Missing if value is absent or null.
   * @throws ConfigException.WrongType if value is not convertible to a Path.
   */
  default Path getPath(String path) {
    String setting = getString(path);
    try {
      return ConfigUtils.resolvePath(setting);
    } catch (InvalidPathException e) {
      throw new ConfigException.WrongType(
          origin(), String.format("%s: Expecting valid filepath, got '%s'", path, setting), e);
    }
  }

  /**
   * Returns the {@link URL} object at the given path.
   *
   * <p>The value will be first interpreted directly as a URL; if the parsing fails, the value will
   * be then interpreted as a path on the local filesystem, then converted to a file URL.
   *
   * <p>If the value is "-" map it to "std:/", to indicate this url represents stdout (when
   * unloading) and stdin (when loading).
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
      return ConfigUtils.resolveURL(setting);
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(),
          String.format("%s: Expecting valid filepath or URL, got '%s'", path, setting),
          e);
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
    try {
      return ConfigUtils.resolveThreads(setting);
    } catch (Exception e) {
      throw new ConfigException.WrongType(
          origin(),
          String.format("%s: Expecting integer or string in 'nC' syntax, got '%s'", path, setting),
          e);
    }
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
          origin(), String.format("%s: Expecting single char, got '%s'", path, setting));
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
          origin(), String.format("%s: Expecting valid charset name, got '%s'", path, setting), e);
    }
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
