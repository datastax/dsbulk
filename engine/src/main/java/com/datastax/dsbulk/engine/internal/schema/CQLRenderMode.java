/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

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
