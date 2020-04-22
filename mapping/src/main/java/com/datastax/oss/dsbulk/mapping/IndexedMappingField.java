/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;

/** A field in a mapping declaration, identified by a zero-based integer index. */
public class IndexedMappingField extends DefaultIndexedField implements MappingField {

  public IndexedMappingField(int index) {
    super(index);
  }
}
