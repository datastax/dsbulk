/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

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
      throw new IllegalArgumentException(
          String.format(
              "Execution ID '%s' is not a valid path name on the local filesytem", executionId),
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
