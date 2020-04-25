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
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtils.class);

  /**
   * Creates a simple keyspace with SimpleStrategy and replication factor 1.
   *
   * @param session The session to use.
   * @param keyspace The keyspace name to create. Should be quoted if necessary.
   */
  public static void createSimpleKeyspace(CqlSession session, String keyspace) {
    try {
      LOGGER.debug("Using keyspace " + keyspace);
      session.execute(CQLUtils.createKeyspaceSimpleStrategy(keyspace, 1));
    } catch (AlreadyExistsException e) {
      LOGGER.warn("Keyspace {} already exists, ignoring", keyspace);
    }
  }

  /**
   * Tests fail randomly with InvalidQueryException: Keyspace 'xxx' does not exist; this method
   * tries at most 3 times to issue a successful USE statement.
   *
   * @param session The session to use.
   * @param keyspace The keyspace to USE.
   */
  public static void useKeyspace(CqlSession session, String keyspace) {
    int maxTries = 3;
    for (int i = 1; i <= maxTries; i++) {
      try {
        session.execute("USE " + keyspace);
      } catch (InvalidQueryException e) {
        if (i == maxTries) {
          throw e;
        }
        LOGGER.error("Could not USE keyspace, retrying");
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Executes the given statements with the given session object.
   *
   * @param session The session to use.
   * @param statements The statements to execute.
   */
  public static void execute(CqlSession session, String... statements) {
    execute(session, Arrays.asList(statements));
  }

  /**
   * Executes the given statements with the given session object.
   *
   * @param session The session to use.
   * @param statements The statements to execute.
   */
  private static void execute(CqlSession session, Collection<String> statements) {
    for (String stmt : statements) {
      session.execute(stmt);
    }
  }
}
