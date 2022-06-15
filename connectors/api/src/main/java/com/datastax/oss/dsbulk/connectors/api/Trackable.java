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
package com.datastax.oss.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.URI;

public interface Trackable {

  /**
   * Returns the record source, typically a line in a file or a row in a database table.
   *
   * @return The record source; may be null if the source cannot be determined or should not be
   *     retained.
   */
  @Nullable
  Object getSource();

  /**
   * Returns the record's resource location, typically the file or database table where it was
   * extracted from.
   *
   * <p>Details about the returned URI, and in particular its scheme, are implementation-specific.
   *
   * @return The record's resource.
   */
  @NonNull
  URI getResource();

  /**
   * Returns the record's position inside its {@linkplain #getResource() resource}, typically the
   * line number if the resource is a file, or the row number, if the resource is a database table.
   *
   * <p>Note that in the case of a file, the record's position is not necessarily equal to the line
   * number where it appears. First, if there is one or more header lines, the actual line number
   * would be its position + the number of header lines. Moreover, it the resource contains
   * multi-line records, then it is not possible to draw a direct relationship between the record's
   * position and its line.
   *
   * <p>Positions should be 1-based, i.e., the first record in the resource has position 1, and so
   * on. If the position cannot be determined, this method should return -1.
   *
   * @return the record's position, or -1 if the position cannot be determined.
   */
  long getPosition();
}
