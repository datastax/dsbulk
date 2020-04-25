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
package com.datastax.oss.dsbulk.mapping;

public enum CQLRenderMode {

  /**
   * Render the fragment as an assignment to a named bound variable (for INSERT, UPDATE and DELETE
   * statements). Usually this renders as {@code :"MyVar"}.
   */
  NAMED_ASSIGNMENT,

  /**
   * Render the fragment as an assignment to a positional bound variable (for INSERT, UPDATE and
   * DELETE statements). Usually this renders as {@code ?}.
   *
   * <p>This render mode is not used anymore and was only useful when connecting with protocol
   * version 1.
   */
  POSITIONAL_ASSIGNMENT,

  /**
   * Render the fragment as an unaliased selector (for SELECT statements). Usually this renders as
   * {@code "MyVar"}.
   *
   * <p>This render mode is not used anymore and was only useful when connecting with protocol *
   * version 1.
   */
  UNALIASED_SELECTOR,

  /**
   * Render the fragment as a selector with an optional alias, if required (for SELECT statements).
   * Usually this renders as {@code "MyVar" AS "MyVar"}.
   */
  ALIASED_SELECTOR,

  /**
   * Render the fragment as a CQL identifier, for example, to reference a named bound variable.
   * Usually this renders as {@code "MyVar"}.
   */
  VARIABLE,

  /**
   * Render the fragment in internal form; for identifiers, this is the form stored in the driver's
   * schema metadata and in the system tables, e.g. {@code MyVar}.
   */
  INTERNAL
}
