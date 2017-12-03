/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** */
public class FileUtils {

  public static List<String> readLines(Path... files) {
    return readLinesAsStream(files).collect(Collectors.toList());
  }

  private static Stream<String> readLinesAsStream(Path... files) {
    return Arrays.stream(files)
        .flatMap(
            path -> {
              try {
                return Files.lines(path);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
  }

  public static String readFile(Path file, Charset charset) throws IOException {
    return Files.readAllLines(file, charset)
        .stream()
        .collect(Collectors.joining(System.lineSeparator()));
  }
}
