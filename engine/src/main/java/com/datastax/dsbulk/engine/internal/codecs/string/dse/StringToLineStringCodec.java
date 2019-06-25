/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string.dse;

import com.datastax.driver.dse.geometry.LineString;
import com.datastax.driver.dse.geometry.codecs.LineStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.util.List;

public class StringToLineStringCodec extends StringConvertingCodec<LineString> {

  public StringToLineStringCodec(List<String> nullStrings) {
    super(LineStringCodec.INSTANCE, nullStrings);
  }

  @Override
  public LineString externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parseLineString(s);
  }

  @Override
  public String internalToExternal(LineString value) {
    if (value == null) {
      return nullString();
    }
    return value.asWellKnownText();
  }
}
