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
