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
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;

public class NumberToStringCodec<EXTERNAL extends Number>
    extends ConvertingCodec<EXTERNAL, String> {
  private final FastThreadLocal<NumberFormat> numberFormat;

  public NumberToStringCodec(Class<EXTERNAL> javaType, FastThreadLocal<NumberFormat> numberFormat) {
    super(TypeCodecs.TEXT, javaType);
    this.numberFormat = numberFormat;
  }

  @Override
  public EXTERNAL internalToExternal(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from string to number");
  }

  @Override
  public String externalToInternal(EXTERNAL value) {
    if (value == null) {
      return null;
    }
    return CodecUtils.formatNumber(value, numberFormat.get());
  }
}
