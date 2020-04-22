/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.mapping;

/**
 * A fragment of CQL language included in a mapping definition or in a CQL query. Fragments can be
 * {@linkplain CQLWord CQL identifiers}, {@linkplain FunctionCall function calls}, or {@linkplain
 * CQLLiteral CQL literals}. In a mapping definition, they appear on the right side of a mapping
 * entry.
 */
public interface CQLFragment extends MappingToken {

  String render(CQLRenderMode mode);
}
