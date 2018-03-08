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

  private final List<String> nullWords;

  public StringToInetAddressCodec(List<String> nullWords) {
    super(inet(), String.class);
    this.nullWords = nullWords;
  }

  @Override
  public InetAddress externalToInternal(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
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
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    return value.getHostAddress();
  }
}
