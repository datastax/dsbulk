/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;

public abstract class ConvertingCodec<FROM, TO> extends TypeCodec<FROM> {

  final TypeCodec<TO> targetCodec;

  protected ConvertingCodec(TypeCodec<TO> targetCodec, Class<FROM> javaType) {
    super(targetCodec.getCqlType(), javaType);
    this.targetCodec = targetCodec;
  }

  TypeToken<TO> getTargetJavaType() {
    return targetCodec.getJavaType();
  }

  @Override
  public ByteBuffer serialize(FROM s, ProtocolVersion protocolVersion) throws InvalidTypeException {
    TO value = convertFrom(s);
    return targetCodec.serialize(value, protocolVersion);
  }

  @Override
  public FROM deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
      throws InvalidTypeException {
    TO value = targetCodec.deserialize(bytes, protocolVersion);
    return convertTo(value);
  }

  @Override
  public FROM parse(String s) throws InvalidTypeException {
    TO value = targetCodec.parse(s);
    return convertTo(value);
  }

  @Override
  public String format(FROM s) throws InvalidTypeException {
    TO value = convertFrom(s);
    return targetCodec.format(value);
  }

  protected abstract TO convertFrom(FROM s);

  protected abstract FROM convertTo(TO value);
}
