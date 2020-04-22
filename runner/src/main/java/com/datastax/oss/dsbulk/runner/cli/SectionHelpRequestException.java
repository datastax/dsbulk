/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.runner.cli;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** Simple exception indicating that the user wants the help for a particular section. */
public class SectionHelpRequestException extends Exception {

  private final String sectionName;
  @Nullable private final String connectorName;

  public SectionHelpRequestException(@NonNull String sectionName, @Nullable String connectorName) {
    this.sectionName = sectionName;
    this.connectorName = connectorName;
  }

  public String getSectionName() {
    return sectionName;
  }

  @Nullable
  public String getConnectorName() {
    return connectorName;
  }
}
