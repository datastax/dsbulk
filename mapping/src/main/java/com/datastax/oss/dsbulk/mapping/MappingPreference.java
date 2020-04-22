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
 * How to determine if a mapping should be considered indexed or mapped in case of ambiguities, and
 * according to the connector's capabilities.
 */
public enum MappingPreference {
  MAPPED_ONLY,
  INDEXED_ONLY,
  MAPPED_OR_INDEXED
}
