/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import java.util.List;
import java.util.function.Predicate;

public class ThrowableUtils {

  private static final Predicate<Throwable> INCLUDE_ALL = t -> true;

  /**
   * Returns {@code true} when the given throwable, its cause, or any of its suppressed errors is an
   * {@link InterruptedException} or any of its common variants. Returns {@code false} otherwise.
   *
   * <p>This is mainly motivated by the fact the DSBulk engine uses the Reactor framework, which
   * sometimes wraps {@link InterruptedException} inside other exceptions, making it hard to detect
   * when the operation has been interrupted.
   *
   * @param throwable The throwable to inspect.
   * @return true if interrupted, false otherwise.
   */
  @SuppressWarnings("UnstableApiUsage")
  public static boolean isInterrupted(@NonNull Throwable throwable) {
    List<Throwable> chain = Throwables.getCausalChain(throwable);
    for (Throwable t : chain) {
      if (checkInterrupted(t)) {
        return true;
      }
      Throwable[] suppressed = t.getSuppressed();
      for (Throwable s : suppressed) {
        if (checkInterrupted(s)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean checkInterrupted(Throwable t) {
    return t instanceof InterruptedException
        || t instanceof InterruptedIOException
        || t instanceof ClosedByInterruptException
        || t instanceof FileLockInterruptionException;
  }

  /**
   * Returns a sanitized, prettified error message for the given throwable. The message is suitable
   * for printing to the console and gives the user the best possible knowledge of the root cause.
   *
   * @param error The throwable to extract a sanitized message from.
   * @return The sanitized message.
   */
  @NonNull
  public static String getSanitizedErrorMessage(@NonNull Throwable error) {
    return getSanitizedErrorMessage(error, INCLUDE_ALL, 2);
  }

  /**
   * Returns a sanitized, prettified error message for the given throwable. The message is suitable
   * for printing to the console and gives the user the best possible knowledge of the root cause.
   *
   * @param error The throwable to extract a sanitized message from.
   * @param filter A filter to eliminate unwanted/unnecessary errors.
   * @param indentation The number of spaces to add when indenting a new line of message.
   * @return The sanitized message.
   */
  @NonNull
  public static String getSanitizedErrorMessage(
      @NonNull Throwable error, @NonNull Predicate<Throwable> filter, int indentation) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printErrorMessage(error, filter, pw, indentation, 0);
    return sw.toString();
  }

  private static void printErrorMessage(
      Throwable error,
      Predicate<Throwable> filter,
      PrintWriter pw,
      int indentation,
      int currentIndentation) {
    pw.print(sanitizedErrorMessage(error));
    Throwable[] suppressed = error.getSuppressed();
    for (Throwable s : suppressed) {
      if (filter.test(s)) {
        newLineAndIndent(pw, currentIndentation + indentation);
        pw.print(" Suppressed: ");
        printErrorMessage(s, filter, pw, indentation, currentIndentation + indentation);
      }
    }
    Throwable cause = error.getCause();
    if (cause != null && cause != error && filter.test(cause)) {
      newLineAndIndent(pw, currentIndentation + indentation);
      pw.print(" Caused by: ");
      printErrorMessage(cause, filter, pw, indentation, currentIndentation + indentation);
    }
  }

  private static String sanitizedErrorMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    if (t instanceof FileNotFoundException) {
      sb.append("File not found: ").append(t.getMessage()).append('.');
    } else if (t instanceof MalformedURLException) {
      sb.append("Malformed URL: ").append(t.getMessage()).append('.');
    } else if (t instanceof UnknownHostException) {
      sb.append("Unknown host: ").append(t.getMessage()).append('.');
    } else if (t instanceof UnsupportedEncodingException) {
      sb.append("Unsupported encoding: ").append(t.getMessage()).append('.');
    } else if (t instanceof AccessDeniedException) {
      sb.append("Access denied: ").append(t.getMessage()).append('.');
    } else if (t instanceof DirectoryNotEmptyException) {
      sb.append("Directory is not empty: ").append(t.getMessage()).append('.');
    } else if (t instanceof FileAlreadyExistsException) {
      sb.append("File already exists: ").append(t.getMessage()).append('.');
    } else if (t instanceof NoSuchFileException) {
      sb.append("No such file: ").append(t.getMessage()).append('.');
    } else if (t instanceof NotDirectoryException) {
      sb.append("File is not a directory: ").append(t.getMessage()).append('.');
    } else if (t instanceof ClosedChannelException) {
      sb.append("Channel is closed.");
    } else if (t instanceof NonReadableChannelException) {
      sb.append("Channel is not readable.");
    } else if (t instanceof NonWritableChannelException) {
      sb.append("Channel is not writable.");
    } else if (t.getMessage() == null
        || t.getMessage().isEmpty()
        || t.getMessage().equals("null")) {
      sb.append(t.getClass().getSimpleName()).append(" (no message).");
    } else if (t.getMessage().matches("\\d+") || t.getMessage().length() < 10) {
      // Message contains only numbers (e.g. for ArrayIndexOutOfBoundsException) or
      // message is too short: append the class name as well for clarity.
      sb.append(t.getClass().getSimpleName()).append(": ");
      appendMessage(t, sb);
    } else {
      appendMessage(t, sb);
    }
    return sb.toString();
  }

  private static void appendMessage(Throwable t, StringBuilder sb) {
    String msg = t.getMessage();
    sb.append(msg.substring(0, 1).toUpperCase());
    if (msg.length() > 1) {
      sb.append(msg.substring(1));
    }
    if (!msg.endsWith(".")) {
      sb.append('.');
    }
  }

  private static void newLineAndIndent(PrintWriter pw, int spaces) {
    pw.println();
    for (int i = 0; i < spaces; i++) {
      pw.print(' ');
    }
  }
}
