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
package com.datastax.oss.dsbulk.runner.help;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class HelpEntry {

  @Nullable private final String shortOption;
  @Nullable private final String abbreviatedOption;
  @Nullable private final String longOption;
  @Nullable private final String argumentType;
  @NonNull private final String description;

  public HelpEntry(
      @Nullable String shortOption,
      @Nullable String abbreviatedOption,
      @Nullable String longOption,
      @Nullable String argumentType,
      @NonNull String description) {
    this.shortOption = shortOption;
    this.abbreviatedOption = abbreviatedOption;
    this.longOption = longOption;
    this.argumentType = argumentType;
    this.description = description;
  }

  @Nullable
  public String getShortOption() {
    return shortOption;
  }

  @Nullable
  public String getAbbreviatedOption() {
    return abbreviatedOption;
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
