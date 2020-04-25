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
package com.datastax.oss.dsbulk.workflow.commons.utils;

import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import reactor.core.Disposable;

public class CloseableUtils {

  public static Exception closeQuietly(AutoCloseable closeable, Exception suppressed) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        if (suppressed != null) {
          e.addSuppressed(suppressed);
        }
        return e;
      }
    }
    return suppressed;
  }

  public static Exception closeQuietly(Disposable disposable, Exception suppressed) {
    if (disposable != null && !disposable.isDisposed()) {
      try {
        disposable.dispose();
      } catch (Exception e) {
        // Reactor framework often wraps InterruptedException
        Throwable root = Throwables.getRootCause(e);
        if (root instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        } else {
          if (suppressed != null) {
            e.addSuppressed(suppressed);
          }
          return e;
        }
      }
    }
    return suppressed;
  }
}
