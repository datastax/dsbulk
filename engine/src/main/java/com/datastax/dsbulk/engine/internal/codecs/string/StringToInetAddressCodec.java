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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.net.InetAddress;
import java.util.List;

public class StringToInetAddressCodec extends ConvertingCodec<String, InetAddress> {

  private final List<String> nullStrings;

  public StringToInetAddressCodec(List<String> nullStrings) {
    super(inet(), String.class);
    this.nullStrings = nullStrings;
  }

  @Override
  public InetAddress externalToInternal(String s) {
    if (s == null || s.isEmpty() || nullStrings.contains(s)) {
      return null;
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
      return nullStrings.isEmpty() ? null : nullStrings.get(0);
    }
    return value.getHostAddress();
  }
}
