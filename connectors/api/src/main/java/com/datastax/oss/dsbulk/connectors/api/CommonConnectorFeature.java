/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.connectors.api;

public enum CommonConnectorFeature implements ConnectorFeature {

  /**
   * Indicates that the connector handles indexed records (i.e. records whose field identifiers are
   * zero-based indices).
   */
  INDEXED_RECORDS,

  /**
   * Indicates that the connector handles mapped records (i.e. records whose field identifiers are
   * strings).
   */
  MAPPED_RECORDS,
}
