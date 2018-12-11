/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.connectors.api.Field;

/**
 * A field in a mapping definition. Fields can be {@linkplain IndexedMappingField indexed} or
 * {@linkplain MappedMappingField named} (mapped). In a mapping definition, they appear on the left
 * side of a mapping entry.
 */
public interface MappingField extends MappingToken, Field {}
