/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string.dse;

import com.datastax.dsbulk.commons.codecs.string.StringConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dse.driver.api.core.codec.DseTypeCodecs;
import com.datastax.dse.driver.api.core.type.geometry.LineString;
import java.util.List;

public class StringToLineStringCodec extends StringConvertingCodec<LineString> {

  public StringToLineStringCodec(List<String> nullStrings) {
    super(DseTypeCodecs.LINE_STRING, nullStrings);
  }

  @Override
  public LineString externalToInternal(String s) {
    if (isNull(s)) {
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
