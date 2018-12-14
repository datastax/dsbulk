/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import org.jetbrains.annotations.NotNull;

/** A field in a mapping definition identified by an alphanumeric name. */
public class MappedMappingField extends DefaultMappedField implements MappingField {

  public MappedMappingField(@NotNull String name) {
    super(name);
  }
}
