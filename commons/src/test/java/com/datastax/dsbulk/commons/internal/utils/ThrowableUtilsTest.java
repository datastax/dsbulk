/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class ThrowableUtilsTest {

  @Test
  void should_detect_interrupted() {
    assertThat(ThrowableUtils.isInterrupted(new InterruptedException())).isTrue();
    assertThat(ThrowableUtils.isInterrupted(new Exception(new InterruptedException()))).isTrue();
    assertThat(ThrowableUtils.isInterrupted(new Exception(new ClosedByInterruptException())))
        .isTrue();
    assertThat(ThrowableUtils.isInterrupted(new Exception(new InterruptedIOException()))).isTrue();
    assertThat(ThrowableUtils.isInterrupted(new Exception(new FileLockInterruptionException())))
        .isTrue();
    Exception e = new Exception();
    e.addSuppressed(new InterruptedException());
    assertThat(ThrowableUtils.isInterrupted(e)).isTrue();
    assertThat(ThrowableUtils.isInterrupted(new RuntimeException())).isFalse();
  }

  @Test
  void should_create_sanitized_error_message() {
    Exception root = new Exception("root", new Exception("cause"));
    root.addSuppressed(new Exception("suppressed1", new Exception("suppressed1 cause")));
    root.addSuppressed(new Exception("suppressed2", new Exception("suppressed2 cause")));
    assertThat(ThrowableUtils.getSanitizedErrorMessage(root))
        .isEqualTo(
            String.format(
                "Root.%n"
                    + "   Suppressed: Suppressed1.%n"
                    + "     Caused by: Suppressed1 cause.%n"
                    + "   Suppressed: Suppressed2.%n"
                    + "     Caused by: Suppressed2 cause.%n"
                    + "   Caused by: Cause."));
  }

  @Test
  void should_create_sanitized_error_message_with_filter() {
    RuntimeException root = new RuntimeException("root", new RuntimeException("cause"));
    root.addSuppressed(new Exception("suppressed1", new RuntimeException("suppressed1 cause")));
    root.addSuppressed(
        new RuntimeException("suppressed2", new RuntimeException("suppressed2 cause")));
    assertThat(ThrowableUtils.getSanitizedErrorMessage(root, RuntimeException.class::isInstance, 4))
        .isEqualTo(
            String.format(
                "Root.%n"
                    + "     Suppressed: Suppressed2.%n"
                    + "         Caused by: Suppressed2 cause.%n"
                    + "     Caused by: Cause."));
  }

  @ParameterizedTest(name = "Message for {0} should contain \"{1}\"")
  @ArgumentsSource(Exceptions.class)
  void should_create_sanitized_error_message_for_special_errors(Throwable error, String expected) {
    assertThat(ThrowableUtils.getSanitizedErrorMessage(error)).contains(expected);
  }

  private static class Exceptions implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      List<Arguments> args = new ArrayList<>();
      args.add(Arguments.of(new RuntimeException(), "RuntimeException (no message)."));
      args.add(Arguments.of(new RuntimeException(""), "RuntimeException (no message)."));
      args.add(
          Arguments.of(new NullPointerException("null"), "NullPointerException (no message)."));
      args.add(Arguments.of(new FileNotFoundException("boo"), "File not found: boo."));
      args.add(Arguments.of(new MalformedURLException("boo"), "Malformed URL: boo."));
      args.add(Arguments.of(new UnknownHostException("boo"), "Unknown host: boo."));
      args.add(Arguments.of(new UnsupportedEncodingException("boo"), "Unsupported encoding: boo."));
      args.add(Arguments.of(new AccessDeniedException("boo"), "Access denied: boo."));
      args.add(Arguments.of(new DirectoryNotEmptyException("boo"), "Directory is not empty: boo."));
      args.add(Arguments.of(new FileAlreadyExistsException("boo"), "File already exists: boo."));
      args.add(Arguments.of(new NoSuchFileException("boo"), "No such file: boo."));
      args.add(Arguments.of(new NotDirectoryException("boo"), "File is not a directory: boo."));
      args.add(Arguments.of(new ClosedChannelException(), "Channel is closed."));
      args.add(Arguments.of(new NonReadableChannelException(), "Channel is not readable."));
      args.add(Arguments.of(new NonWritableChannelException(), "Channel is not writable."));
      return args.stream();
    }
  }
}
