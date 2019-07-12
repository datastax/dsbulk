package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OperationDirectoryResolverTest {

  private Path tempFolder;

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
    Files.createDirectories(tempFolder);
  }

  @AfterEach
  void deleteTempFolder() {
    deleteDirectory(tempFolder);
  }

  @Test
  void should_create_operation_directory_when_does_not_exist() throws IOException {
    OperationDirectoryResolver resolver =
        new OperationDirectoryResolver(tempFolder, "TEST_EXECUTION_ID");
    assertThat(resolver.resolve()).exists().isWritable();
  }

  @Test
  void should_return_existing_operation_directory() throws IOException {
    Path operationDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Files.createDirectories(operationDir);
    OperationDirectoryResolver resolver =
        new OperationDirectoryResolver(tempFolder, "TEST_EXECUTION_ID");
    assertThat(resolver.resolve()).exists().isWritable();
  }

  @Test
  void should_throw_ISE_when_operation_directory_not_empty() throws Exception {
    Path operationDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Path foo = operationDir.resolve("foo");
    Files.createDirectories(foo);
    OperationDirectoryResolver resolver =
        new OperationDirectoryResolver(tempFolder, "TEST_EXECUTION_ID");
    assertThatThrownBy(resolver::resolve)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Operation directory exists but is not empty: " + operationDir);
  }

  @Test
  void should_throw_ISE_when_operation_directory_not_writable() {
    assumingThat(
        !PlatformUtils.isWindows(),
        () -> {
          Path operationDir = tempFolder.resolve("TEST_EXECUTION_ID");
          Files.createDirectories(operationDir);
          assertThat(operationDir.toFile().setWritable(false, false)).isTrue();
          OperationDirectoryResolver resolver =
              new OperationDirectoryResolver(tempFolder, "TEST_EXECUTION_ID");
          assertThatThrownBy(resolver::resolve)
              .isInstanceOf(IllegalStateException.class)
              .hasMessage("Operation directory exists but is not writable: " + operationDir);
        });
  }

  @Test
  void should_throw_ISE_when_operation_directory_not_directory() throws Exception {
    Path operationDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Files.createFile(operationDir);
    OperationDirectoryResolver resolver =
        new OperationDirectoryResolver(tempFolder, "TEST_EXECUTION_ID");
    assertThatThrownBy(resolver::resolve)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Operation directory exists but is not a directory: " + operationDir);
  }

  @Test
  void should_throw_ISE_when_operation_directory_contains_forbidden_chars() {
    OperationDirectoryResolver resolver =
        new OperationDirectoryResolver(tempFolder, "invalid file \u0000 name");
    assertThatThrownBy(resolver::resolve)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Execution ID 'invalid file \u0000 name' is not a valid path name on the local filesytem");
  }
}
