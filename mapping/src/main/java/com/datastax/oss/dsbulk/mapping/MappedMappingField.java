/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A field in a mapping definition identified by an alphanumeric name. */
public class MappedMappingField extends DefaultMappedField implements MappingField {

  public MappedMappingField(@NonNull String name) {
    super(name);
  }
}
