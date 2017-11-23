/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.ccm;

/** An exception wrapping all errors raised when a CCM command fails to execute properly. */
public class CCMException extends RuntimeException {

  private final String command;
  private final String out;
  private final String err;

  public CCMException(String message, String command, String out, String err) {
    super(message);
    this.command = command;
    this.out = out;
    this.err = err;
  }

  public CCMException(String message, String command, String out, String err, Throwable cause) {
    super(message, cause);
    this.command = command;
    this.out = out;
    this.err = err;
  }

  /**
   * Returns the CCM command that failed to execute.
   *
   * @return the CCM command that failed to execute.
   */
  public String getCommand() {
    return command;
  }

  /**
   * Returns the CCM command standard output, or an empty string if there was no standard output.
   *
   * @return the CCM command standard output.
   */
  public String getOut() {
    return out;
  }

  /**
   * Returns the CCM command error output, or an empty string if there was no error output.
   *
   * @return the CCM command error output.
   */
  public String getErr() {
    return err;
  }
}
