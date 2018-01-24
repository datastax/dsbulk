/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;

public class ZipUtils {

  /**
   * Unzips the specified zip file to the specified destination directory. Replaces any files in the
   * destination, if they already exist.
   */
  public static void unzip(String src, Path dest) throws IOException {
    if (Files.notExists(dest)) {
      Files.createDirectories(dest);
    }
    URI uri = URI.create("jar:" + ClassLoader.getSystemResource(src));
    try (FileSystem zipFileSystem = FileSystems.newFileSystem(uri, new HashMap<>())) {
      Path root = zipFileSystem.getPath("/");
      Files.walkFileTree(
          root,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Path destFile = Paths.get(dest.toString(), file.toString());
              Files.copy(file, destFile, REPLACE_EXISTING);
              return CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              Path dirToCreate = Paths.get(dest.toString(), dir.toString());
              if (Files.notExists(dirToCreate)) {
                Files.createDirectory(dirToCreate);
              }
              return CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
