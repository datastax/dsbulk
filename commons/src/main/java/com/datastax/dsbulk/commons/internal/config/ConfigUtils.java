/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import static com.datastax.dsbulk.commons.config.LoaderConfig.LEAF_ANNOTATION;
import static com.datastax.dsbulk.commons.config.LoaderConfig.TYPE_ANNOTATION;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigException.Missing;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class ConfigUtils {

  private static final Pattern THREADS_PATTERN =
      Pattern.compile("(.+)\\s*C", Pattern.CASE_INSENSITIVE);

  private static final Pattern WRONG_TYPE_PATTERN =
      Pattern.compile(" has type (\\w+) rather than (\\w+)", Pattern.CASE_INSENSITIVE);

  private static final Pattern ENUM_PATTERN =
      Pattern.compile(
          "The enum class \\w+ has no constant of the name ('.+') \\(should be one of \\[([^]]+)]\\.\\)",
          Pattern.CASE_INSENSITIVE);

  public static BulkConfigurationException configExceptionToBulkConfigurationException(
      ConfigException e, String path) {
    if (e instanceof ConfigException.WrongType) {
      // This will happen if a user provides the wrong type, e.g. a string where a number was
      // expected. We remove the origin's description as it is too cryptic for users.
      // Error generated will look like this:
      // "connector.csv.recursive: Expecting X, got Y."
      String em = e.getMessage();
      int startingIndex = e.origin().description().length() + 2;
      String errorMsg = "Invalid value for " + path + "." + em.substring(startingIndex);
      Matcher matcher = WRONG_TYPE_PATTERN.matcher(errorMsg);
      if (matcher.find()) {
        errorMsg = matcher.replaceAll(": Expecting $2, got $1");
      }
      return new BulkConfigurationException(errorMsg, e);
    } else {
      // Catch-all for other types of exceptions.
      // We intercept errors related to unknown enum constants to improve the error message,
      // which will look like this:
      // log.stmt.level: 1: Invalid value at 'stmt.level': Expecting one of X, Y, Z, got 'weird'
      String errorMsg = e.getMessage();
      Matcher matcher = ENUM_PATTERN.matcher(errorMsg);
      if (matcher.find()) {
        errorMsg = matcher.replaceAll("Expecting one of $2, got $1");
      }
      return new BulkConfigurationException(errorMsg, e);
    }
  }

  /**
   * Resolves the given path.
   *
   * <p>The returned path is normalized and absolute. If the input denotes a relative path, it is
   * resolved against the current working directory. If it starts with a tilde, the tilde is
   * expanded into the current user's home directory.
   *
   * @param path The path to resolve.
   * @return The resolved {@link Path}, absolute and normalized.
   * @throws InvalidPathException If the path cannot be resolved.
   */
  public static Path resolvePath(String path) throws InvalidPathException {
    if (path.startsWith("~")) {
      if (path.equals("~") || path.startsWith("~/")) {
        path = System.getProperty("user.home") + path.substring(1);
      } else {
        throw new InvalidPathException(path, "Cannot resolve home directory", 1);
      }
    }
    return Paths.get(path).toAbsolutePath().normalize();
  }

  /**
   * Resolves the given URL.
   *
   * <p>The returned URL is normalized.
   *
   * <p>This method first tries to interpret the input as a valid URL, possibly expanding the
   * special {@code "-"} (single dash) URL into DSBulk's internal {@link
   * LoaderURLStreamHandlerFactory#STD standard input/output} URL.
   *
   * <p>If that fails, this method then attempts to interpret the input as a path object. See {@link
   * #resolvePath(String)}.
   *
   * @param url The URL to resolve.
   * @return The resolved {@link URL}, normalized.
   * @throws MalformedURLException If the URL cannot be resolved.
   * @throws InvalidPathException If the path cannot be resolved.
   */
  public static URL resolveURL(String url) throws MalformedURLException, InvalidPathException {
    if (url.equals("-")) {
      url = "std:/";
    }
    try {
      return new URL(url).toURI().normalize().toURL();
    } catch (Exception e) {
      // not a valid URL, consider it a path on the local filesystem.
      try {
        return resolvePath(url).toUri().toURL();
      } catch (MalformedURLException | InvalidPathException e1) {
        e1.addSuppressed(e);
        throw e1;
      }
    }
  }

  /**
   * Resolves the given input as a positive integer representing the number of threads to allocate.
   *
   * <p>This method first tries to parse the input directly as an integer.
   *
   * <p>If that fails, it then tries to parse the input as an integer followed by the letter 'C'. If
   * that succeeds, the total number of threads returned is <code>
   * n * {@link Runtime#availableProcessors() number of available cores}</code>.
   *
   * @param threadsStr The string to parse.
   * @throws PatternSyntaxException If the input cannot be parsed.
   * @throws IllegalArgumentException If the input can be parsed, but the resulting integer is not
   *     positive.
   * @return The number of threads.
   */
  public static int resolveThreads(String threadsStr) {
    int threads;
    try {
      threads = Integer.parseInt(threadsStr);
    } catch (NumberFormatException e) {
      Matcher matcher = THREADS_PATTERN.matcher(threadsStr.trim());
      if (matcher.matches()) {
        threads =
            (int)
                (((float) Runtime.getRuntime().availableProcessors())
                    * Float.parseFloat(matcher.group(1)));
        return Math.max(1, threads);
      } else {
        PatternSyntaxException e1 =
            new PatternSyntaxException(
                "Cannot parse input as N * <num_cores>", THREADS_PATTERN.pattern(), 0);
        e1.addSuppressed(e);
        throw e1;
      }
    }
    if (threads < 1) {
      throw new IllegalArgumentException("Expecting positive number of threads, got " + threads);
    }
    return threads;
  }

  /**
   * Returns a string representation of the value type at this path.
   *
   * @param config the config.
   * @param path path expression.
   * @return the type string
   * @throws ConfigException.Missing if value is absent.
   */
  public static String getTypeString(Config config, String path) {
    ConfigValue value = getNullSafeValue(config, path);
    Optional<String> typeHint = getTypeHint(value);
    if (typeHint.isPresent()) {
      return typeHint.get();
    }
    ConfigValueType type = value.valueType();
    if (type == ConfigValueType.LIST) {
      ConfigList list = config.getList(path);
      if (list.isEmpty()) {
        return "list";
      } else {
        return "list<" + getTypeString(list.get(0).valueType()) + ">";
      }
    } else if (type == ConfigValueType.OBJECT) {
      ConfigObject object = config.getObject(path);
      if (object.isEmpty()) {
        return "map";
      } else {
        return "map<string," + getTypeString(object.values().iterator().next().valueType()) + ">";
      }
    } else {
      return getTypeString(type);
    }
  }

  /**
   * Alternative to {@link ConfigValue#valueType()} that honors any type hints found in the
   * configuration, if any.
   *
   * @param config the config.
   * @param path path expression.
   * @return the {@link ConfigValueType value type}.
   * @throws ConfigException.Missing if value is absent.
   */
  public static ConfigValueType getValueType(Config config, String path) {
    ConfigValue value = getNullSafeValue(config, path);
    Optional<String> typeHint = getTypeHint(value);
    if (typeHint.isPresent()) {
      String hint = typeHint.get();
      if (hint.equals("string")) {
        return ConfigValueType.STRING;
      }
      if (hint.equals("number")) {
        return ConfigValueType.NUMBER;
      }
      if (hint.equals("boolean")) {
        return ConfigValueType.BOOLEAN;
      }
      if (hint.startsWith("list")) {
        return ConfigValueType.LIST;
      }
      if (hint.startsWith("map")) {
        return ConfigValueType.OBJECT;
      }
    }
    return value.valueType();
  }

  /**
   * Retrieves the type hint for the given value, if any.
   *
   * @param value the {@link ConfigValue value} to inspect.
   * @return The type hint, if any, or empty otherwise.
   */
  public static Optional<String> getTypeHint(ConfigValue value) {
    return value
        .origin()
        .comments()
        .stream()
        .filter(line -> line.contains(TYPE_ANNOTATION))
        .map(line -> line.replace("@type", ""))
        .map(String::trim)
        .findFirst();
  }

  /**
   * Returns the comments associated with the given value, excluding type hints.
   *
   * @param value the {@link ConfigValue value} to inspect.
   * @return The comments associated with the given value
   */
  @NotNull
  public static String getComments(ConfigValue value) {
    return value
        .origin()
        .comments()
        .stream()
        .filter(line -> !line.contains(TYPE_ANNOTATION))
        .map(String::trim)
        .collect(Collectors.joining("\n"));
  }

  /**
   * Return a string representation of the given value type.
   *
   * @param type ConfigValueType to stringify.
   * @return the type string
   */
  private static String getTypeString(ConfigValueType type) {
    switch (type) {
      case STRING:
        return "string";
      case LIST:
        return "list";
      case OBJECT:
        return "map";
      case NUMBER:
        return "number";
      case BOOLEAN:
        return "boolean";
      default:
        return "arg";
    }
  }

  /**
   * Returns true if the given value is a leaf.
   *
   * <p>Leaf values are values of all types except OBJECT, or values of type OBJECT explicitly
   * annotated with @leaf.
   *
   * @param value The value to inspect.
   * @return True if the value is a leaf, false otherwise.
   */
  public static boolean isLeaf(ConfigValue value) {
    return !(value instanceof ConfigObject)
        || value.origin().comments().stream().anyMatch(line -> line.contains(LEAF_ANNOTATION));
  }

  /**
   * An alternative to {@link Config#getValue(String)} that handles null values gracefully instead
   * of throwing.
   *
   * <p>Note that the path must still exist; if the path does not exist (i.e., the value is
   * completely absent from the config object), this method still throws {@link Missing}.
   *
   * @param config The config object to get the value from.
   * @param path The path at which the value is to be found.
   * @return The {@link ConfigValue value}.
   * @throws Missing If the path is not present in the config object.
   */
  public static ConfigValue getNullSafeValue(Config config, String path) {
    int dot = path.indexOf('.');
    if (dot == -1) {
      ConfigValue value = config.root().get(path);
      if (value == null) {
        throw new Missing(path);
      }
      return value;
    } else {
      try {
        return getNullSafeValue(config.getConfig(path.substring(0, dot)), path.substring(dot + 1));
      } catch (Missing e) {
        throw new Missing(path);
      }
    }
  }
}
