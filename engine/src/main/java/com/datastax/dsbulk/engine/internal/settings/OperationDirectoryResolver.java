package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public class OperationDirectoryResolver {

  private final Path logDirectory;
  private final String executionId;

  public OperationDirectoryResolver(Path logDirectory, String executionId) {
    this.logDirectory = logDirectory;
    this.executionId = executionId;
  }

  public Path resolve() throws IOException {
    Path operationDirectory;
    try {
      operationDirectory = logDirectory.resolve(executionId);
    } catch (InvalidPathException e) {
      throw new BulkConfigurationException(
          String.format(
              "Operation ID '%s' is not a valid path name on the local filesytem", executionId),
          e);
    }
    if (Files.exists(operationDirectory)) {
      if (Files.isDirectory(operationDirectory)) {
        if (Files.isWritable(operationDirectory)) {
          @SuppressWarnings("StreamResourceLeak")
          long count = Files.list(operationDirectory).count();
          if (count > 0) {
            throw new IllegalStateException(
                "Operation directory exists but is not empty: " + operationDirectory);
          }
        } else {
          throw new IllegalStateException(
              "Operation directory exists but is not writable: " + operationDirectory);
        }
      } else {
        throw new IllegalStateException(
            "Operation directory exists but is not a directory: " + operationDirectory);
      }
    } else {
      Files.createDirectories(operationDirectory);
    }
    return operationDirectory;
  }
}
