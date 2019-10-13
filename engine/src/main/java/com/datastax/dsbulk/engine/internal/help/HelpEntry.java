/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.help;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class HelpEntry {

  @Nullable private final String shortOption;
  @Nullable private final String longOption;
  @Nullable private final String argumentType;
  @NonNull private final String description;

  public HelpEntry(
      @Nullable String shortOption,
      @Nullable String longOption,
      @Nullable String argumentType,
      @NonNull String description) {
    this.shortOption = shortOption;
    this.longOption = longOption;
    this.argumentType = argumentType;
    this.description = description;
  }

  @Nullable
  public String getShortOption() {
    return shortOption;
  }

  @Nullable
  public String getLongOption() {
    return longOption;
  }

  @Nullable
  public String getArgumentType() {
    return argumentType;
  }

  @NonNull
  public String getDescription() {
    return description;
  }
}
