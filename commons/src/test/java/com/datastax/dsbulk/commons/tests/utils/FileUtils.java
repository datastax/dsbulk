/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtils {

  public static String readFile(Path file) throws IOException {
    return readFile(file, StandardCharsets.UTF_8);
  }

  public static String readFile(Path file, Charset charset) throws IOException {
    return Files.readAllLines(file, charset).stream()
        .collect(Collectors.joining(System.lineSeparator()));
  }

  public static List<Path> listAllFilesInDirectory(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.collect(Collectors.toList());
    }
  }

  public static Stream<String> readAllLinesInDirectoryAsStream(Path dir) throws IOException {
    return readAllLinesInDirectoryAsStream(dir, StandardCharsets.UTF_8);
  }

  public static Stream<String> readAllLinesInDirectoryAsStream(Path dir, Charset charset)
      throws IOException {
    try (Stream<Path> files = Files.walk(dir)) {
      List<String> lines =
          files
              .filter(Files::isRegularFile)
              .flatMap(path -> readAllLines(path, charset))
              .collect(Collectors.toList());
      return lines.stream();
    }
  }

  public static Stream<String> readAllLinesInDirectoryAsStreamExcludingHeaders(Path dir)
      throws IOException {
    return readAllLinesInDirectoryAsStreamExcludingHeaders(dir, StandardCharsets.UTF_8);
  }

  public static Stream<String> readAllLinesInDirectoryAsStreamExcludingHeaders(
      Path dir, Charset charset) throws IOException {
    return listAllFilesInDirectory(dir).stream()
        .flatMap(file -> readAllLines(file, charset).skip(1));
  }

  public static Stream<String> readAllLines(Path path) {
    return readAllLines(path, StandardCharsets.UTF_8);
  }

  public static Stream<String> readAllLines(Path path, Charset charset) {
    try {
      return Files.readAllLines(path, charset).stream();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void deleteDirectory(Path dir) {
    try {
      Files.walkFileTree(
          dir,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              // Do not delete directories on Windows as deletes are not executed immediately,
              // but they do result in an IO exception the next time we try to access a directory in
              // pending-deletion state. Leaving directories empty is good enough for DSBulk anyway,
              // it only complains when directories are not empty.
              if (!PlatformUtils.isWindows()) {
                Files.delete(dir);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ignored) {
    }
  }

  public static Path createURLFile(URL... urls) throws IOException {
    File file = File.createTempFile("urlfile", null);
    Files.write(
        file.toPath(),
        Arrays.stream(urls).map(URL::toExternalForm).collect(Collectors.toList()),
        StandardCharsets.US_ASCII);
    return file.toPath();
  }

  public static Path createURLFile(List<String> urls) throws IOException {
    File file = File.createTempFile("urlfile", null);
    Files.write(file.toPath(), urls, StandardCharsets.US_ASCII);
    return file.toPath();
  }
}
