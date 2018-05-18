/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.net.InetAddress;
import java.util.List;

public class StringToInetAddressCodec extends StringConvertingCodec<InetAddress> {

  public StringToInetAddressCodec(List<String> nullStrings) {
    super(inet(), nullStrings);
  }

  @Override
  public InetAddress externalToInternal(String s) {
    if (isNull(s)) {
      return null;
    }
    if (s.isEmpty()) {
      throw new InvalidTypeException("Cannot create inet address from empty string");
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new InvalidTypeException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public String internalToExternal(InetAddress value) {
    if (value == null) {
      return nullString();
    }
    return value.getHostAddress();
  }
}
