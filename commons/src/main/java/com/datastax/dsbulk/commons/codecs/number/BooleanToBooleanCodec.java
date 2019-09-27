/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

public class BooleanToBooleanCodec extends ConvertingCodec<Boolean, Boolean> {

  public BooleanToBooleanCodec() {
    super(TypeCodecs.BOOLEAN, Boolean.class);
  }

  @Override
  public Boolean internalToExternal(Boolean value) {
    return value;
  }

  @Override
  public Boolean externalToInternal(Boolean value) {
    return value;
  }
}
