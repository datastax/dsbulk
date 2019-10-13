/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.cli;

import edu.umd.cs.findbugs.annotations.Nullable;

/** Simple exception indicating that the user wants the main help output. */
public class GlobalHelpRequestException extends Exception {

  private final String connectorName;

  GlobalHelpRequestException() {
    this(null);
  }

  GlobalHelpRequestException(@Nullable String connectorName) {
    this.connectorName = connectorName;
  }

  @Nullable
  public String getConnectorName() {
    return connectorName;
  }
}
