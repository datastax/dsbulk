/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtils {

  public static String readFile(Path file) throws IOException {
    return readFile(file, StandardCharsets.UTF_8);
  }

  public static String readFile(Path file, Charset charset) throws IOException {
    return Files.readAllLines(file, charset)
        .stream()
        .collect(Collectors.joining(System.lineSeparator()));
  }

  public static List<Path> listAllFilesInDirectory(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.collect(Collectors.toList());
    }
  }

  public static Stream<String> readAllLinesInDirectoryAsStream(Path dir, Charset charset)
      throws IOException {
    try (Stream<Path> files = Files.walk(dir)) {
      List<String> lines =
          files
              .filter(Files::isRegularFile)
              .flatMap(
                  path -> {
                    try {
                      return Files.readAllLines(path, charset).stream();
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  })
              .collect(Collectors.toList());
      return lines.stream();
    }
  }

  public static void deleteDirectory(Path dir) {
    try {
      deleteRecursively(dir, ALLOW_INSECURE);
    } catch (IOException ignored) {
    }
  }
}
