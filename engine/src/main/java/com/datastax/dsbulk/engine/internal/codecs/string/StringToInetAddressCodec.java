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

public class StringToInetAddressCodec extends ConvertingCodec<String, InetAddress> {

  public static final StringToInetAddressCodec INSTANCE = new StringToInetAddressCodec();

  private StringToInetAddressCodec() {
    super(inet(), String.class);
  }

  @Override
  public InetAddress convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new InvalidTypeException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public String convertTo(InetAddress value) {
    if (value == null) {
      return null;
    }
    return value.getHostAddress();
  }
}
