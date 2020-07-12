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
package com.datastax.oss.dsbulk.runner;

import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_ABORTED_FATAL_ERROR;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_CRASHED;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_INTERRUPTED;

import com.datastax.oss.dsbulk.commons.ThrowableUtils;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

  /** A filter to exclude some errors from the sanitized message printed to the console. */
  private static final Predicate<Throwable> THROWABLE_PREDICATE =
      t -> {
        // filter out Reactor exceptions as they are usually not relevant to users;
        // other more meaningful errors are generally present.
        if (t.getClass().getName().startsWith("reactor.")) {
          return false;
        }
        // Reactor throws "java.lang.Exception: #block terminated with an error" when
        // blocking for a result; this is usually not meaningful to users.
        if (t.getMessage() != null) {
          return !t.getMessage().contains("#block terminated");
        }
        // All other cases: retain it.
        return true;
      };

  @NonNull
  public static ExitStatus handleUnexpectedError(
      @Nullable Workflow workflow, @NonNull Throwable error) {
    // Reactor framework often wraps InterruptedException.
    if (ThrowableUtils.isInterrupted(error)) {
      return STATUS_INTERRUPTED;
    } else {
      String errorMessage = ThrowableUtils.getSanitizedErrorMessage(error, THROWABLE_PREDICATE, 2);
      String operationName = workflow == null ? "Operation" : workflow.toString();
      if (error instanceof Exception) {
        LOGGER.error(operationName + " failed: " + errorMessage, error);
        return STATUS_ABORTED_FATAL_ERROR;
      } else {
        LOGGER.error(operationName + " failed unexpectedly: " + errorMessage, error);
        return STATUS_CRASHED;
      }
    }
  }
}
