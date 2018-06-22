/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;

// Dummy impl until the dse driver has the real impl
public class DateRangeCodec implements TypeCodec<DateRange> {

  @Override
  public GenericType<DateRange> getJavaType() {
    return null;
  }

  @Override
  public DataType getCqlType() {
    return null;
  }

  @Override
  public ByteBuffer encode(DateRange value, ProtocolVersion protocolVersion) {
    return null;
  }

  @Override
  public DateRange decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return null;
  }

  @Override
  public String format(DateRange value) {
    return null;
  }

  @Override
  public DateRange parse(String value) {
    return null;
  }
}
