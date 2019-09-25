/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string.dse;

import com.datastax.driver.dse.geometry.Point;
import com.datastax.driver.dse.geometry.codecs.PointCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.util.List;

public class StringToPointCodec extends StringConvertingCodec<Point> {

  public StringToPointCodec(List<String> nullStrings) {
    super(PointCodec.INSTANCE, nullStrings);
  }

  @Override
  public Point externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parsePoint(s);
  }

  @Override
  public String internalToExternal(Point value) {
    if (value == null) {
      return nullString();
    }
    return value.asWellKnownText();
  }
}
