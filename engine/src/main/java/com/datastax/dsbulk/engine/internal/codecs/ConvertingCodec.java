/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;

/**
 * A codec that converts to and from an external representation of a value (i.e., the value, as
 * produced by a connector) and its internal representation (i.e., the Java type that matches the
 * target CQL type).
 *
 * @param <EXTERNAL> The external type (as produced and consumed by the connector)
 * @param <INTERNAL> The internal type (the one that matches the target CQL type)
 */
public abstract class ConvertingCodec<EXTERNAL, INTERNAL> extends TypeCodec<EXTERNAL> {

  protected final TypeCodec<INTERNAL> internalCodec;

  protected ConvertingCodec(TypeCodec<INTERNAL> internalCodec, Class<EXTERNAL> javaType) {
    super(internalCodec.getCqlType(), javaType);
    this.internalCodec = internalCodec;
  }

  public ConvertingCodec(TypeCodec<INTERNAL> internalCodec, TypeToken<EXTERNAL> javaType) {
    super(internalCodec.getCqlType(), javaType);
    this.internalCodec = internalCodec;
  }

  public TypeCodec<INTERNAL> getInternalCodec() {
    return internalCodec;
  }

  public TypeToken<INTERNAL> getInternalJavaType() {
    return internalCodec.getJavaType();
  }

  @Override
  public ByteBuffer serialize(EXTERNAL external, ProtocolVersion protocolVersion)
      throws InvalidTypeException {
    INTERNAL value = externalToInternal(external);
    return internalCodec.serialize(value, protocolVersion);
  }

  @Override
  public EXTERNAL deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
      throws InvalidTypeException {
    INTERNAL internal = internalCodec.deserialize(bytes, protocolVersion);
    return internalToExternal(internal);
  }

  // parse and format are implemented out of completeness, but are never used

  @Override
  public EXTERNAL parse(String s) throws InvalidTypeException {
    INTERNAL value = internalCodec.parse(s);
    return internalToExternal(value);
  }

  @Override
  public String format(EXTERNAL s) throws InvalidTypeException {
    INTERNAL value = externalToInternal(s);
    return internalCodec.format(value);
  }

  /**
   * Converts the external representation of a value (i.e., the value, as produced by a connector)
   * to its internal representation (i.e., the Java type that matches the target CQL type).
   *
   * @param external the value's external form.
   * @return the value's internal form.
   */
  public abstract INTERNAL externalToInternal(EXTERNAL external);

  /**
   * Converts the internal representation of a value (i.e., the Java type that matches the target
   * CQL type) to its external representation (i.e., the value, as consumed by a connector).
   *
   * @param internal the value's internal form.
   * @return the value's external form.
   */
  public abstract EXTERNAL internalToExternal(INTERNAL internal);
}
