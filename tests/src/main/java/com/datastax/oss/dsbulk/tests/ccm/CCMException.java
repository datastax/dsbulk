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
package com.datastax.oss.dsbulk.tests.ccm;

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
