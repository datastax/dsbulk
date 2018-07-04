/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.google.common.util.concurrent.Uninterruptibles;
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
